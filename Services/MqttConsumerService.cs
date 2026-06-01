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
    private int _lastChannelOccupancy;   // protected by Interlocked in UpdateChannelMetrics

    // ── Worker pool ───────────────────────────────────────────────────────────
    // Workers use CancellationToken.None on ReadAllAsync so they drain the channel
    // fully on shutdown. Scale-down is cooperative: a worker checks _targetWorkerCount
    // before picking up each message and exits if there are too many workers.
    private readonly List<Task> _workers = [];
    private readonly SemaphoreSlim _workerLock = new(1, 1);
    private int _activeWorkerCount;
    private volatile int _targetWorkerCount;

    // Stored once ExecuteAsync starts; passed to ProcessMessageAsync so long-running
    // handlers can observe application shutdown without blocking the drain loop.
    private CancellationToken _stoppingToken;

    // ── Resilience pipelines ──────────────────────────────────────────────────
    // One pipeline per registered topic filter; key is the filter string.
    // Each pipeline = Retry (exponential backoff) → CircuitBreaker.
    private readonly Dictionary<string, ResiliencePipeline> _pipelines = new();
    private static readonly ResiliencePropertyKey<string> TopicKey = new("mqtt.topic");

    // ── MQTT client ───────────────────────────────────────────────────────────
    private readonly IMqttClient _mqttClient;

    // Signals ConnectLoopAsync that the broker disconnected.
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

        var workerOpts = _workerOptsMonitor.CurrentValue;
        await ScaleWorkersToAsync(workerOpts.InitialWorkerCount, stoppingToken);

        _ = AdaptiveScalerAsync(stoppingToken);
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
            // Fresh TCS per attempt — the previous one stays completed after disconnect
            // and would make WhenAny return immediately on the next iteration.
            _disconnectedTcs = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously);

            try
            {
                _logger.LogInformation("Connecting to MQTT broker {Host}:{Port} (attempt {Attempt})",
                    _mqttOpts.Host, _mqttOpts.Port, attempt + 1);

                await _mqttClient.ConnectAsync(options, ct);
                attempt = 0; // reset backoff counter on successful connect

                foreach (var filter in _router.RegisteredFilters)
                {
                    await _mqttClient.SubscribeAsync(
                        new MqttClientSubscribeOptionsBuilder()
                            .WithTopicFilter(filter, MqttQualityOfServiceLevel.AtLeastOnce)
                            .Build(), ct);
                    _logger.LogInformation("Subscribed to {Filter}", filter);
                }

                // Block until disconnected or application stopping
                await Task.WhenAny(_disconnectedTcs.Task, Task.Delay(Timeout.Infinite, ct));
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "MQTT connection failed");
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
        // Backpressure: when the channel is near full, stop ACKing QoS ≥1 messages.
        // The broker will hold and retransmit them, pushing pressure to the network layer.
        if (_backpressureActive)
        {
            e.AutoAcknowledge = false;
            return;
        }

        var topic         = e.ApplicationMessage.Topic;
        var correlationId = ExtractCorrelationId(e.ApplicationMessage);

        using var receiveActivity = MqttActivitySource.StartReceive(topic, correlationId);

        // Capture the ActivityContext struct while the span is still alive.
        // The struct remains valid after the activity is disposed — it holds
        // only the TraceId/SpanId/TraceFlags values needed to parent child spans.
        var parentContext = receiveActivity?.Context ?? default;

        var payload = e.ApplicationMessage.PayloadSegment.ToArray();

        _metrics.MessagesReceived.Add(1, new TagList { { "topic", topic } });

        var message = new MqttMessage(
            topic,
            payload,
            e.ApplicationMessage.QualityOfServiceLevel,
            e.ApplicationMessage.Retain,
            correlationId,
            DateTimeOffset.UtcNow,
            parentContext);

        using var enqueueActivity = MqttActivitySource.StartEnqueue(topic, correlationId);

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
            // CancellationToken.None: workers drain all remaining messages on shutdown.
            // Per-message scale-down is cooperative (checked below).
            await foreach (var message in _channel.Reader.ReadAllAsync(CancellationToken.None))
            {
                // Cooperative scale-down: if we have more workers than the current target,
                // exit after finishing this message. Remaining messages are picked up by
                // the surviving workers. During drain, _targetWorkerCount = int.MaxValue
                // so no worker exits prematurely.
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

        // Pass the topic through Polly context so OnRetry callbacks can log it
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
            // Circuit is open — don't retry; route to DLC so the main pipeline keeps moving
            processActivity?.RecordException(bcx);
            _logger.LogWarning(bcx, "Circuit open for topic {Topic}, routing to dead-letter",
                message.Topic);
            await SendToDeadLetterAsync(message, bcx, "circuit-open");
        }
        catch (Exception ex)
        {
            // All Polly retries exhausted
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

    private async Task AdaptiveScalerAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(
            TimeSpan.FromMilliseconds(_workerOptsMonitor.CurrentValue.ScaleCheckIntervalMs));

        while (!ct.IsCancellationRequested && await timer.WaitForNextTickAsync(ct))
        {
            var opts     = _workerOptsMonitor.CurrentValue; // live config reload
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
                // Write the new target; workers check it cooperatively before each message
                Volatile.Write(ref _targetWorkerCount, target);
            }
        }
    }

    private async Task ScaleWorkersToAsync(int target, CancellationToken ct)
    {
        await _workerLock.WaitAsync(ct);
        try
        {
            // Prune completed tasks to prevent the list growing unboundedly
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

        // Disable cooperative scale-down so every worker drains to completion
        Volatile.Write(ref _targetWorkerCount, int.MaxValue);

        _logger.LogInformation("Shutdown: draining {Count} messages from channel",
            _channel.Reader.Count);

        // Signal workers that no more messages will arrive
        _channel.Writer.Complete();

        using var timeout = new CancellationTokenSource(
            TimeSpan.FromSeconds(workerOpts.ShutdownTimeoutSeconds));
        try
        {
            await Task.WhenAll(_workers).WaitAsync(timeout.Token);
            _logger.LogInformation("All workers drained cleanly");
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning(
                "Shutdown timeout ({Timeout}s) expired; {Remaining} messages remain in channel",
                workerOpts.ShutdownTimeoutSeconds,
                _channel.Reader.Count);
        }

        if (_mqttClient.IsConnected)
            await _mqttClient.DisconnectAsync();
    }

    // ── Resilience pipeline builder ──────────────────────────────────────────

    private void BuildPipelines(WorkerPoolOptions opts)
    {
        ResiliencePipeline Build(string filterName) =>
            new ResiliencePipelineBuilder()
                // Retry wraps the circuit breaker: failures are retried before CB records them.
                // BrokenCircuitException is explicitly excluded from retry so a tripped CB
                // immediately propagates to the catch block in ProcessMessageAsync.
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
                        _logger.LogWarning(
                            "Circuit OPENED for filter {Filter} (break {Dur}s)",
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
        // Interlocked.Exchange returns the OLD value, giving us the delta atomically
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

    // Exponential backoff with ±20% jitter, capped at maxDelaySeconds.
    private static TimeSpan ExponentialBackoff(int attempt, int baseSeconds, int maxSeconds)
    {
        var raw    = baseSeconds * Math.Pow(2, attempt - 1);
        var capped = Math.Min(raw, maxSeconds);
        var jitter = capped * 0.2 * (Random.Shared.NextDouble() - 0.5); // ±10%
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
