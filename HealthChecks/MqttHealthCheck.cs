using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using MqttClient.Configuration;

namespace MqttClient.HealthChecks;

public sealed class MqttHealthCheck : IHealthCheck
{
    private readonly MqttConnectionState _state;
    // IOptionsMonitor so the threshold reflects hot-reloaded config,
    // not the snapshot captured at construction time.
    private readonly IOptionsMonitor<WorkerPoolOptions> _opts;

    public MqttHealthCheck(MqttConnectionState state, IOptionsMonitor<WorkerPoolOptions> opts)
    {
        _state = state;
        _opts  = opts;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct)
    {
        var opts = _opts.CurrentValue;

        var data = new Dictionary<string, object>
        {
            ["connected"]        = _state.IsConnected,
            ["channel_fill_pct"] = _state.ChannelFillPercent,
            ["worker_count"]     = _state.ActiveWorkers,
        };

        if (!_state.IsConnected)
            return Task.FromResult(HealthCheckResult.Unhealthy("MQTT broker disconnected", data: data));

        if (_state.ChannelFillPercent >= opts.BackpressureHighWatermarkPercent)
            return Task.FromResult(HealthCheckResult.Degraded("Channel near capacity", data: data));

        return Task.FromResult(HealthCheckResult.Healthy(data: data));
    }
}

/// <summary>
/// Shared state between MqttConsumerService and the health check.
/// All writes use Interlocked/volatile to guarantee cross-thread visibility.
/// </summary>
public sealed class MqttConnectionState
{
    private volatile int _isConnected;
    private volatile int _channelFillPercent;
    private volatile int _activeWorkers;

    public bool IsConnected
    {
        get => _isConnected == 1;
        set => Interlocked.Exchange(ref _isConnected, value ? 1 : 0);
    }

    public int ChannelFillPercent
    {
        get => Volatile.Read(ref _channelFillPercent);
        set => Interlocked.Exchange(ref _channelFillPercent, value);
    }

    public int ActiveWorkers
    {
        get => Volatile.Read(ref _activeWorkers);
        set => Interlocked.Exchange(ref _activeWorkers, value);
    }
}
