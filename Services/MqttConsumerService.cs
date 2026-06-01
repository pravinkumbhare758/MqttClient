using System.Diagnostics;
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
    private readonly WorkerPoolOptions _workerOpts;
    private readonly ITopicRouter _router;
    private readonly DeadLetterService _dlc;
    private readonly MqttMetrics _metrics;
    private readonly MqttConnectionState _connState;
    private readonly IOptionsMonitor<WorkerPoolOptions> _workerOptsMonitor;

    // Bounded channel – BoundedChannelFullMode.Wait provides internal backpressure
    private readonly Channel<MqttMessage> _channel;
    private int _lastChannelOccupancy;

    private readonly IMqttClient _mqttClient;
    private readonly SemaphoreSlim _workerLock = new(1, 1);
    private readonly List<Task> _workers = [];
    private int _activeWorkerCount;

    // Circuit breakers keyed by topic filter (one per registered handler)
    private readonly Dictionary<string, ResiliencePipeline> _circuitBreakers = new();

    // Set when channel fill exceeds high-water mark; stops ACKing MQTT messages
    private volatile bool _backpressureActive;

    // Signals the connect loop that MQTT has disconnected; recreated per connection attempt
    private TaskCompletionSource<bool> _disconnectedTcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

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
        _workerOpts        = workerOptsMonitor.CurrentValue;
        _router            = router;
        _dlc               = dlc;
        _metrics           = metrics;
        _connState         = connState;

        _channel = Channel.CreateBounded<MqttMessage>(new BoundedChannelOptions(_workerOpts.ChannelCapacity)
        {
            FullMode     = BoundedChannelFullMode.Wait,
            SingleWriter = false,
            SingleReader = false
        });

        _mqttClient = new MqttFactory().CreateMqttClient();
        _mqttClient.ApplicationMessageReceivedAsync += OnMessageReceivedAsync;
        _mqttClient.ConnectedAsync                  += OnConnectedAsync;
        _mqttClient.DisconnectedAsync               += OnDisconnectedAsync;

        BuildCircuitBreakers();
    }

    // ── BackgroundService ────────────────────────────────────────────────────

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await ScaleWorkersToAsync(_workerOpts.InitialWorkerCount, stoppingToken);
        _ = AdaptiveScalerAsync(stoppingToken);
        await ConnectLoopAsync(stoppingToken);
        await DrainAsync();
    }

    // ── MQTT connection loop ─────────────────────────────────────────────────

    private async Task ConnectLoopAsync(CancellationToken ct)
    {
        var options = BuildMqttOptions();

        while (!ct.IsCancellationRequested)
        {
            // Fresh TCS for each connection attempt so WaitForDisconnect works on reconnect
            _disconnectedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            try
            {
                _logger.LogInformation("Connecting to MQTT broker {Host}:{Port}", _mqttOpts.Host, _mqttOpts.Port);
                await _mqttClient.ConnectAsync(options, ct);

                foreach (var filter in _router.RegisteredFilters)
                {
                    await _mqttClient.SubscribeAsync(
                        new MqttClientSubscribeOptionsBuilder()
                            .WithTopicFilter(filter, MqttQualityOfServiceLevel.AtLeastOnce)
                            .Build(), ct);
                    _logger.LogInformation("Subscribed to {Filter}", filter);
                }

                // Block here until broker disconnects or app is stopping
                await Task.WhenAny(_disconnectedTcs.Task, Task.Delay(Timeout.Infinite, ct));
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "MQTT connection failed, retrying in {Delay}s",
                    _mqttOpts.ReconnectDelaySeconds);
            }

            if (!ct.IsCancellationRequested)
                await Task.Delay(TimeSpan.FromSeconds(_mqttOpts.ReconnectDelaySeconds), ct)
                          .ConfigureAwait(false);
        }
    }

    // ── MQTT event callbacks ─────────────────────────────────────────────────

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
        // Step 11: Backpressure – stop ACKing when channel is near capacity.
        // The broker retains unacknowledged messages and retransmits after reconnect.
        if (_backpressureActive)
        {
            e.AutoAcknowledge = false;
            return;
        }

        var topic         = e.ApplicationMessage.Topic;
        var correlationId = ExtractCorrelationId(e.ApplicationMessage);

        using var receiveActivity = MqttActivitySource.StartReceive(topic, correlationId);

        // Copy payload from the broker's buffer before it is returned to the pool
        var payload = e.ApplicationMessage.PayloadSegment.ToArray();

        _metrics.MessagesReceived.Add(1, new TagList { { "topic", topic } });

        var message = new MqttMessage(
            topic,
            payload,
            e.ApplicationMessage.QualityOfServiceLevel,
            e.ApplicationMessage.Retain,
            correlationId,
            DateTimeOffset.UtcNow,
            Activity.Current);

        using var _ = MqttActivitySource.StartEnqueue(topic, correlationId);

        await _channel.Writer.WriteAsync(message).ConfigureAwait(false);
        UpdateChannelMetrics();
    }

    // ── Worker pool ──────────────────────────────────────────────────────────

    private async Task WorkerLoopAsync(CancellationToken ct)
    {
        _metrics.WorkerCount.Add(1);
        Interlocked.Increment(ref _activeWorkerCount);

        try
        {
            await foreach (var message in _channel.Reader.ReadAllAsync(ct))
            {
                await ProcessMessageAsync(message, ct).ConfigureAwait(false);
                UpdateChannelMetrics();
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        finally
        {
            _metrics.WorkerCount.Add(-1);
            Interlocked.Decrement(ref _activeWorkerCount);
        }
    }

    private async Task ProcessMessageAsync(MqttMessage message, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        using var processActivity = MqttActivitySource.StartProcess(
            message.Topic, message.CorrelationId, message.ParentActivity);

        var handler = _router.Resolve(message.Topic);
        if (handler is null)
        {
            _logger.LogWarning("No handler registered for topic {Topic}", message.Topic);
            return;
        }

        var pipeline = _circuitBreakers.GetValueOrDefault(handler.TopicFilter)
                    ?? _circuitBreakers["__default__"];

        try
        {
            await pipeline.ExecuteAsync(async innerCt =>
            {
                await handler.HandleAsync(message, innerCt).ConfigureAwait(false);
            }, ct);

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
            _logger.LogWarning(bcx, "Circuit open for {Topic}, routing to dead-letter", message.Topic);
            processActivity?.RecordException(bcx);
            await SendToDeadLetterAsync(message, bcx, "circuit-open");
        }
        catch (Exception ex) when (message.RetryCount < _workerOpts.MaxRetries)
        {
            _metrics.MessagesRetried.Add(1);
            processActivity?.RecordException(ex);
            _logger.LogWarning(ex, "Retry {Count}/{Max} for topic {Topic}",
                message.RetryCount + 1, _workerOpts.MaxRetries, message.Topic);
            var retried = message with { RetryCount = message.RetryCount + 1 };
            await _channel.Writer.WriteAsync(retried, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            sw.Stop();
            _metrics.MessagesFailed.Add(1, new TagList { { "topic", message.Topic } });
            processActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            processActivity?.RecordException(ex);
            await SendToDeadLetterAsync(message, ex, "max-retries-exceeded");
        }
    }

    // ── Adaptive scaler (Step 1) ─────────────────────────────────────────────

    private async Task AdaptiveScalerAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(
            TimeSpan.FromMilliseconds(_workerOpts.ScaleCheckIntervalMs));

        while (!ct.IsCancellationRequested && await timer.WaitForNextTickAsync(ct))
        {
            // Reads latest config snapshot; supports runtime reload via IOptionsMonitor (Step 7)
            var opts = _workerOptsMonitor.CurrentValue;
            var fill = _channel.Reader.Count * 100 / Math.Max(1, opts.ChannelCapacity);

            _connState.ActiveWorkers      = _activeWorkerCount;
            _connState.ChannelFillPercent = fill;
            _backpressureActive           = fill >= opts.BackpressureHighWatermarkPercent;

            if (fill >= opts.ScaleUpThresholdPercent && _activeWorkerCount < opts.MaxWorkerCount)
            {
                _logger.LogInformation("Channel {Fill}% full – scaling up to {Count} workers",
                    fill, _activeWorkerCount + 1);
                await ScaleWorkersToAsync(_activeWorkerCount + 1, ct);
            }
            else if (fill <= opts.ScaleDownThresholdPercent && _activeWorkerCount > opts.MinWorkerCount)
            {
                _logger.LogDebug("Channel {Fill}% full – below scale-down threshold", fill);
                // Workers stop naturally as the channel drains
            }
        }
    }

    private async Task ScaleWorkersToAsync(int target, CancellationToken ct)
    {
        await _workerLock.WaitAsync(ct);
        try
        {
            while (_activeWorkerCount < target)
                _workers.Add(WorkerLoopAsync(ct));
        }
        finally
        {
            _workerLock.Release();
        }
    }

    // ── Graceful shutdown (Step 6) ───────────────────────────────────────────

    private async Task DrainAsync()
    {
        _logger.LogInformation("Draining {Count} messages from channel...", _channel.Reader.Count);
        _channel.Writer.Complete();

        using var timeout = new CancellationTokenSource(
            TimeSpan.FromSeconds(_workerOpts.ShutdownTimeoutSeconds));

        try
        {
            await Task.WhenAll(_workers).WaitAsync(timeout.Token);
            _logger.LogInformation("All workers drained cleanly");
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning(
                "Shutdown timeout ({Timeout}s) expired; {Remaining} messages remain in channel",
                _workerOpts.ShutdownTimeoutSeconds,
                _channel.Reader.Count);
        }

        if (_mqttClient.IsConnected)
            await _mqttClient.DisconnectAsync();
    }

    // ── Circuit breaker builder (Step 4) ────────────────────────────────────

    private void BuildCircuitBreakers()
    {
        ResiliencePipeline Build() => new ResiliencePipelineBuilder()
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                FailureRatio      = 0.5,
                SamplingDuration  = TimeSpan.FromSeconds(_workerOpts.CircuitBreakerDurationSeconds),
                MinimumThroughput = _workerOpts.CircuitBreakerFailureThreshold,
                BreakDuration     = TimeSpan.FromSeconds(_workerOpts.CircuitBreakerDurationSeconds),
                OnOpened = args =>
                {
                    _logger.LogWarning("Circuit OPENED (break duration {Dur}s)",
                        _workerOpts.CircuitBreakerDurationSeconds);
                    return ValueTask.CompletedTask;
                },
                OnClosed = args =>
                {
                    _logger.LogInformation("Circuit CLOSED – handler recovering");
                    return ValueTask.CompletedTask;
                }
            })
            .Build();

        _circuitBreakers["__default__"] = Build();
        foreach (var filter in _router.RegisteredFilters)
            _circuitBreakers[filter] = Build();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private void UpdateChannelMetrics()
    {
        var current = _channel.Reader.Count;
        var delta   = current - _lastChannelOccupancy;
        if (delta != 0)
        {
            _metrics.ChannelOccupancy.Add(delta);
            _lastChannelOccupancy = current;
        }
    }

    private static string ExtractCorrelationId(MqttApplicationMessage msg)
    {
        if (msg.UserProperties is not null)
        {
            foreach (var prop in msg.UserProperties)
                if (prop.Name.Equals("correlationId", StringComparison.OrdinalIgnoreCase))
                    return prop.Value;
        }
        return Guid.NewGuid().ToString("N");
    }

    private MqttClientOptions BuildMqttOptions()
    {
        var builder = new MqttClientOptionsBuilder()
            .WithTcpServer(_mqttOpts.Host, _mqttOpts.Port)
            .WithClientId(_mqttOpts.ClientId)
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(_mqttOpts.KeepAliveSeconds))
            .WithCleanSession(_mqttOpts.CleanSession)
            .WithSessionExpiryInterval((uint)_mqttOpts.SessionExpiryIntervalSeconds);

        if (_mqttOpts.Username is not null)
            builder = builder.WithCredentials(_mqttOpts.Username, _mqttOpts.Password);

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
