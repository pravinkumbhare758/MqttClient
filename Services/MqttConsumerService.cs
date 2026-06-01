using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using Polly;
using Polly.CircuitBreaker;
using MqttClient.Configuration;
using MqttClient.HealthChecks;
using MqttClient.Interfaces;
using MqttClient.Metrics;
using MqttClient.Models;
using MqttClient.Tracing;

namespace MqttClient.Services;

public sealed class MqttConsumerService : BackgroundService, IAsyncDisposable
{
    private readonly ILogger<MqttConsumerService> _logger;
    private readonly MqttOptions _mqttOpts;
    private readonly ITopicRouter _router;
    private readonly DeadLetterService _dlc;
    private readonly MqttMetrics _metrics;
    private readonly MqttConnectionState _connState;
    private readonly IOptionsMonitor<WorkerPoolOptions> _workerOptsMonitor;

    // ── Channel ──────────────────────────────────────────────────────────────
    private readonly Channel<MqttMessage> _channel;
    private int _lastChannelOccupancy;

    // ── Worker pool ───────────────────────────────────────────────────────────
    private readonly List<Task> _workers = [];
    private readonly SemaphoreSlim _workerLock = new(1, 1);
    private int _activeWorkerCount;
    private volatile int _targetWorkerCount;

    // Stored once ExecuteAsync starts; passed to handlers so they observe shutdown.
    private CancellationToken _stoppingToken;

    // ── Scaler task ───────────────────────────────────────────────────────────
    // Stored so DrainAsync can confirm it exited cleanly (Bug 2 fix).
    private Task _scalerTask = Task.CompletedTask;

    // ── Resilience pipelines ──────────────────────────────────────────────────
    private readonly Dictionary<string, ResiliencePipeline> _pipelines = new();
    private static readonly ResiliencePropertyKey<string> TopicKey = new("mqtt.topic");

    // ── MQTT client ───────────────────────────────────────────────────────────
    private readonly IMqttClient _mqttClient;

    // Recreated at the start of each connection attempt (see ConnectLoopAsync).
    private volatile TaskCompletionSource<bool> _disconnectedTcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private volatile bool _backpressureActive;

    public MqttConsumerService(
        ILogger<MqttConsumerService> logger,
        IOptions<MqttOptions> mqttOpts,
        IOptionsMonitor<WorkerPoolOptions> workerOptsMonitor,
        ITopicRouter router,
        DeadLetterService dlc,
        MqttMetrics metrics,
        MqttConnectionState connState)
    {
        _logger            = logger;
        _mqttOpts          = mqttOpts.Value;
        _workerOptsMonitor = workerOptsMonitor;
        _router            = router;
        _dlc               = dlc;
        _metrics           = metrics;
        _connState         = connState;

        var workerOpts = workerOptsMonitor.CurrentValue;

        _channel = Channel.CreateBounded<MqttMessage>(new BoundedChannelOptions(workerOpts.ChannelCapacity)
        {
            FullMode     = BoundedChannelFullMode.Wait,
            SingleWriter = false,
            SingleReader = false
        });

        _mqttClient = new MqttFactory().CreateMqttClient();
        _mqttClient.ApplicationMessageReceivedAsync += OnMessageReceivedAsync;
        _mqttClient.ConnectedAsync                  += OnConnectedAsync;
        _mqttClient.DisconnectedAsync               += OnDisconnectedAsync;

        BuildPipelines(workerOpts);
    }

    // ── BackgroundService ────────────────────────────────────────────────────

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;

        await ScaleWorkersToAsync(_workerOptsMonitor.CurrentValue.InitialWorkerCount, stoppingToken);

        // BUG FIX 2: Store the scaler task instead of fire-and-forget.
        // RunScalerWithFaultLoggingAsync catches and logs any unexpected exception
        // so a crash in the scaler is visible rather than silently swallowed.
        _scalerTask = RunScalerWithFaultLoggingAsync(stoppingToken);

        await ConnectLoopAsync(stoppingToken);
        await DrainAsync();
    }

    // ── MQTT connection loop ─────────────────────────────────────────────────

    private async Task ConnectLoopAsync(CancellationToken ct)
    {
        int attempt = 0;
        var options = BuildMqttClientOptions();

        while (!ct.IsCancellationRequested)
        {
            _disconnectedTcs = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously);

            try
            {
                _logger.LogInformation("Connecting to MQTT broker {Host}:{Port} (attempt {Attempt})",
                    _mqttOpts.Host, _mqttOpts.Port, attempt + 1);

                await _mqttClient.ConnectAsync(options, ct);
                attempt = 0;

                // BUG FIX 4: Check the SUBACK result code for every subscription.
                // A broker can grant a lower QoS than requested, or reject entirely.
                // Throwing here triggers the reconnect loop so the client retries.
                foreach (var filter in _router.RegisteredFilters)
                {
                    var subResult = await _mqttClient.SubscribeAsync(
                        new MqttClientSubscribeOptionsBuilder()
                            .WithTopicFilter(filter, MqttQualityOfServiceLevel.AtLeastOnce)
                            .Build(), ct);

                    foreach (var item in subResult.Items)
                    {
                        if (item.ResultCode is not (MqttClientSubscribeResultCode.GrantedQoS0
                            or MqttClientSubscribeResultCode.GrantedQoS1
                            or MqttClientSubscribeResultCode.GrantedQoS2))
                        {
                            throw new InvalidOperationException(
                                $"Broker rejected subscription to '{filter}': {item.ResultCode}. " +
                                $"Check broker ACL configuration.");
                        }

                        _logger.LogInformation("Subscribed to {Filter} (granted QoS={QoS})",
                            filter, item.ResultCode);
                    }
                }

                await Task.WhenAny(_disconnectedTcs.Task, Task.Delay(Timeout.Infinite, ct));
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "MQTT connection/subscription failed");
            }

            if (!ct.IsCancellationRequested)
            {
                var delay = ExponentialBackoff(
                    ++attempt,
                    _mqttOpts.ReconnectBaseDelaySeconds,
                    _mqttOpts.ReconnectMaxDelaySeconds);

                _logger.LogInformation("Reconnecting in {Delay:F1}s (attempt {Attempt})",
                    delay.TotalSeconds, attempt);

                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
        }
    }

    // ── MQTT event handlers ──────────────────────────────────────────────────

    private Task OnConnectedAsync(MqttClientConnectedEventArgs _)
    {
        _connState.IsConnected = true;
        _logger.LogInformation("Connected to MQTT broker");
        return Task.CompletedTask;
    }

    private Task OnDisconnectedAsync(MqttClientDisconnectedEventArgs e)
    {
        _connState.IsConnected = false;
        _logger.LogWarning(e.Exception, "Disconnected from MQTT broker");
        _disconnectedTcs.TrySetResult(true);
        return Task.CompletedTask;
    }

    private async Task OnMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs e)
    {
        // BUG FIX 3: Distinguish QoS=0 (no broker retransmit) from QoS≥1 under backpressure.
        // QoS=0 is permanently lost if we don't process it now — log and count it.
        // QoS≥1 is safe to NACK: the broker retains and retransmits after reconnect.
        if (_backpressureActive)
        {
            var topic = e.ApplicationMessage.Topic;
            if (e.ApplicationMessage.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce)
            {
                _metrics.MessagesReceived.Add(1, new TagList { { "topic", topic } });
                _metrics.MessagesFailed.Add(1, new TagList { { "topic", topic } });
                _logger.LogWarning(
                    "QoS=0 message permanently dropped on topic {Topic}: " +
                    "backpressure active and broker will not retransmit",
                    topic);
            }
            else
            {
                e.AutoAcknowledge = false;
            }
            return;
        }

        var msgTopic      = e.ApplicationMessage.Topic;
        var correlationId = ExtractCorrelationId(e.ApplicationMessage);

        using var receiveActivity = MqttActivitySource.StartReceive(msgTopic, correlationId);
        var parentContext = receiveActivity?.Context ?? default;

        var payload = e.ApplicationMessage.PayloadSegment.ToArray();

        _metrics.MessagesReceived.Add(1, new TagList { { "topic", msgTopic } });

        var message = new MqttMessage(
            msgTopic,
            payload,
            e.ApplicationMessage.QualityOfServiceLevel,
            e.ApplicationMessage.Retain,
            correlationId,
            DateTimeOffset.UtcNow,
            parentContext);

        using var enqueueActivity = MqttActivitySource.StartEnqueue(msgTopic, correlationId);

        await _channel.Writer.WriteAsync(message).ConfigureAwait(false);
        UpdateChannelMetrics();
    }

    // ── Worker pool ──────────────────────────────────────────────────────────

    private async Task WorkerLoopAsync()
    {
        Interlocked.Increment(ref _activeWorkerCount);
        _metrics.WorkerCount.Add(1);
        try
        {
            await foreach (var message in _channel.Reader.ReadAllAsync(CancellationToken.None))
            {
                if (Volatile.Read(ref _activeWorkerCount) > Volatile.Read(ref _targetWorkerCount))
                    break;

                await ProcessMessageAsync(message, _stoppingToken).ConfigureAwait(false);
                UpdateChannelMetrics();
            }
        }
        finally
        {
            Interlocked.Decrement(ref _activeWorkerCount);
            _metrics.WorkerCount.Add(-1);
        }
    }

    private async Task ProcessMessageAsync(MqttMessage message, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        using var processActivity = MqttActivitySource.StartProcess(
            message.Topic, message.CorrelationId, message.ParentContext);

        var handler = _router.Resolve(message.Topic);
        if (handler is null)
        {
            _logger.LogWarning("No handler registered for topic {Topic}", message.Topic);
            return;
        }

        var pipeline = _pipelines.GetValueOrDefault(handler.TopicFilter)
                    ?? _pipelines["__default__"];

        var resilienceCtx = ResilienceContextPool.Shared.Get(ct);
        resilienceCtx.Properties.Set(TopicKey, message.Topic);

        try
        {
            await pipeline.ExecuteAsync(async ctx =>
            {
                await handler.HandleAsync(message, ctx.CancellationToken).ConfigureAwait(false);
            }, resilienceCtx);

            sw.Stop();
            var tags = new TagList { { "topic", message.Topic } };
            _metrics.MessagesProcessed.Add(1, tags);
            _metrics.ProcessingLatencyMs.Record(sw.Elapsed.TotalMilliseconds, tags);
            _metrics.EndToEndLatencyMs.Record(
                (DateTimeOffset.UtcNow - message.ReceivedAt).TotalMilliseconds, tags);

            processActivity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (BrokenCircuitException bcx)
        {
            processActivity?.RecordException(bcx);
            _logger.LogWarning(bcx, "Circuit open for topic {Topic}, routing to dead-letter",
                message.Topic);
            await SendToDeadLetterAsync(message, bcx, "circuit-open");
        }
        catch (Exception ex)
        {
            sw.Stop();
            _metrics.MessagesFailed.Add(1, new TagList { { "topic", message.Topic } });
            processActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            processActivity?.RecordException(ex);
            await SendToDeadLetterAsync(message, ex, "max-retries-exceeded");
        }
        finally
        {
            ResilienceContextPool.Shared.Return(resilienceCtx);
        }
    }

    // ── Adaptive scaler ──────────────────────────────────────────────────────

    // BUG FIX 2: Wrapper that logs any unexpected exception from the scaler.
    // Without this, a NullReferenceException or any other fault silently kills
    // scaling with no log, no metric, no indication anything went wrong.
    private async Task RunScalerWithFaultLoggingAsync(CancellationToken ct)
    {
        try
        {
            await AdaptiveScalerAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Normal shutdown path — not an error
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex,
                "AdaptiveScaler stopped unexpectedly — worker pool will no longer scale " +
                "automatically. Restart the service to restore adaptive scaling.");
        }
    }

    private async Task AdaptiveScalerAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(
            TimeSpan.FromMilliseconds(_workerOptsMonitor.CurrentValue.ScaleCheckIntervalMs));

        while (!ct.IsCancellationRequested && await timer.WaitForNextTickAsync(ct))
        {
            var opts     = _workerOptsMonitor.CurrentValue;
            var capacity = opts.ChannelCapacity;
            var fill     = _channel.Reader.Count * 100 / Math.Max(1, capacity);

            _connState.ChannelFillPercent = fill;
            _connState.ActiveWorkers      = Volatile.Read(ref _activeWorkerCount);
            _backpressureActive           = fill >= opts.BackpressureHighWatermarkPercent;

            var current = Volatile.Read(ref _activeWorkerCount);

            if (fill >= opts.ScaleUpThresholdPercent && current < opts.MaxWorkerCount)
            {
                var target = Math.Min(current + 1, opts.MaxWorkerCount);
                _logger.LogInformation(
                    "Channel {Fill}% full – scaling up workers {Current} → {Target}",
                    fill, current, target);
                await ScaleWorkersToAsync(target, ct);
            }
            else if (fill <= opts.ScaleDownThresholdPercent && current > opts.MinWorkerCount)
            {
                var target = Math.Max(current - 1, opts.MinWorkerCount);
                _logger.LogInformation(
                    "Channel {Fill}% full – scaling down workers {Current} → {Target}",
                    fill, current, target);
                Volatile.Write(ref _targetWorkerCount, target);
            }
        }
    }

    private async Task ScaleWorkersToAsync(int target, CancellationToken ct)
    {
        await _workerLock.WaitAsync(ct);
        try
        {
            // Log any workers that faulted before pruning them
            foreach (var faulted in _workers.Where(t => t.IsFaulted))
                _logger.LogError(faulted.Exception, "A consumer worker task faulted unexpectedly");

            _workers.RemoveAll(t => t.IsCompleted);

            Volatile.Write(ref _targetWorkerCount, target);
            while (Volatile.Read(ref _activeWorkerCount) < target)
                _workers.Add(WorkerLoopAsync());
        }
        finally
        {
            _workerLock.Release();
        }
    }

    // ── Graceful shutdown ────────────────────────────────────────────────────

    private async Task DrainAsync()
    {
        var workerOpts = _workerOptsMonitor.CurrentValue;

        Volatile.Write(ref _targetWorkerCount, int.MaxValue);

        _logger.LogInformation("Shutdown: draining {Count} messages from channel",
            _channel.Reader.Count);

        _channel.Writer.Complete();

        // BUG FIX 1: Capture the worker list under the lock before handing it to
        // Task.WhenAll. Without the lock, ScaleWorkersToAsync (running concurrently
        // in the still-live scaler task) can call _workers.RemoveAll while WhenAll
        // is iterating the same List<Task>, causing InvalidOperationException.
        Task[] workerSnapshot;
        await _workerLock.WaitAsync();
        try { workerSnapshot = _workers.ToArray(); }
        finally { _workerLock.Release(); }

        using var timeout = new CancellationTokenSource(
            TimeSpan.FromSeconds(workerOpts.ShutdownTimeoutSeconds));
        try
        {
            await Task.WhenAll(workerSnapshot).WaitAsync(timeout.Token);
            _logger.LogInformation("All workers drained cleanly");
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning(
                "Shutdown timeout ({Timeout}s) expired; {Remaining} messages remain in channel",
                workerOpts.ShutdownTimeoutSeconds,
                _channel.Reader.Count);
        }

        // Wait for the scaler to exit before tearing down resources it might touch
        try { await _scalerTask.WaitAsync(TimeSpan.FromSeconds(2)); }
        catch { /* scaler already logged any fault; don't block shutdown */ }

        if (_mqttClient.IsConnected)
            await _mqttClient.DisconnectAsync();
    }

    // ── Resilience pipeline builder ──────────────────────────────────────────

    private void BuildPipelines(WorkerPoolOptions opts)
    {
        ResiliencePipeline Build(string filterName) =>
            new ResiliencePipelineBuilder()
                .AddRetry(new Polly.Retry.RetryStrategyOptions
                {
                    MaxRetryAttempts = opts.MaxRetries,
                    BackoffType      = DelayBackoffType.Exponential,
                    Delay            = TimeSpan.FromSeconds(1),
                    UseJitter        = true,
                    ShouldHandle     = new PredicateBuilder()
                        .Handle<Exception>(ex => ex is not BrokenCircuitException),
                    OnRetry = args =>
                    {
                        args.Context.Properties.TryGetValue(TopicKey, out var topic);
                        _metrics.MessagesRetried.Add(1);
                        _logger.LogWarning(
                            args.Outcome.Exception,
                            "Retry {Attempt}/{Max} for topic {Topic} (filter={Filter})",
                            args.AttemptNumber + 1, opts.MaxRetries, topic, filterName);
                        return ValueTask.CompletedTask;
                    }
                })
                .AddCircuitBreaker(new CircuitBreakerStrategyOptions
                {
                    FailureRatio      = 0.5,
                    SamplingDuration  = TimeSpan.FromSeconds(opts.CircuitBreakerDurationSeconds),
                    MinimumThroughput = opts.CircuitBreakerFailureThreshold,
                    BreakDuration     = TimeSpan.FromSeconds(opts.CircuitBreakerDurationSeconds),
                    OnOpened = args =>
                    {
                        _logger.LogWarning("Circuit OPENED for filter {Filter} (break {Dur}s)",
                            filterName, opts.CircuitBreakerDurationSeconds);
                        return ValueTask.CompletedTask;
                    },
                    OnClosed = args =>
                    {
                        _logger.LogInformation("Circuit CLOSED for filter {Filter}", filterName);
                        return ValueTask.CompletedTask;
                    }
                })
                .Build();

        _pipelines["__default__"] = Build("__default__");
        foreach (var filter in _router.RegisteredFilters)
            _pipelines[filter] = Build(filter);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private void UpdateChannelMetrics()
    {
        var current  = _channel.Reader.Count;
        var previous = Interlocked.Exchange(ref _lastChannelOccupancy, current);
        var delta    = current - previous;
        if (delta != 0)
            _metrics.ChannelOccupancy.Add(delta);

        var opts = _workerOptsMonitor.CurrentValue;
        _connState.ChannelFillPercent = current * 100 / Math.Max(1, opts.ChannelCapacity);
    }

    private static string ExtractCorrelationId(MqttApplicationMessage msg)
    {
        if (msg.UserProperties is not null)
            foreach (var prop in msg.UserProperties)
                if (prop.Name.Equals("correlationId", StringComparison.OrdinalIgnoreCase))
                    return prop.Value;

        return Guid.NewGuid().ToString("N");
    }

    private static TimeSpan ExponentialBackoff(int attempt, int baseSeconds, int maxSeconds)
    {
        var raw    = baseSeconds * Math.Pow(2, attempt - 1);
        var capped = Math.Min(raw, maxSeconds);
        var jitter = capped * 0.2 * (Random.Shared.NextDouble() - 0.5);
        return TimeSpan.FromSeconds(Math.Max(1, capped + jitter));
    }

    private MqttClientOptions BuildMqttClientOptions()
    {
        var builder = new MqttClientOptionsBuilder()
            .WithTcpServer(_mqttOpts.Host, _mqttOpts.Port)
            .WithClientId(_mqttOpts.ClientId)
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(_mqttOpts.KeepAliveSeconds))
            .WithCleanSession(_mqttOpts.CleanSession)
            .WithSessionExpiryInterval((uint)_mqttOpts.SessionExpiryIntervalSeconds);

        if (_mqttOpts.Username is not null)
            builder = builder.WithCredentials(_mqttOpts.Username, _mqttOpts.Password);

        if (_mqttOpts.UseTls)
        {
            var tlsOptions = new MqttClientTlsOptions
            {
                UseTls                    = true,
                AllowUntrustedCertificates = _mqttOpts.AllowUntrustedCertificates
            };

            if (_mqttOpts.ClientCertificatePath is not null)
            {
                var cert = new X509Certificate2(
                    _mqttOpts.ClientCertificatePath,
                    _mqttOpts.ClientCertificatePassword);
                tlsOptions.ClientCertificatesProvider =
                    new DefaultMqttCertificatesProvider([cert]);
            }

            builder = builder.WithTls(tlsOptions);
        }

        return builder.Build();
    }

    private Task SendToDeadLetterAsync(MqttMessage message, Exception ex, string reason) =>
        _dlc.EnqueueAsync(new DeadLetterMessage(message, reason, ex, DateTimeOffset.UtcNow))
            .AsTask();

    // ── IAsyncDisposable ─────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        await _dlc.DisposeAsync();
        _mqttClient.Dispose();
        _workerLock.Dispose();
        base.Dispose();
    }
}
