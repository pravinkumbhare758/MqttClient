using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using MqttClient.Configuration;

namespace MqttClient.HealthChecks;

public sealed class MqttHealthCheck : IHealthCheck
{
    private readonly MqttConnectionState _state;
    private readonly WorkerPoolOptions _opts;

    public MqttHealthCheck(MqttConnectionState state, IOptions<WorkerPoolOptions> opts)
    {
        _state = state;
        _opts  = opts.Value;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct)
    {
        var data = new Dictionary<string, object>
        {
            ["connected"]        = _state.IsConnected,
            ["channel_fill_pct"] = _state.ChannelFillPercent,
            ["worker_count"]     = _state.ActiveWorkers,
        };

        if (!_state.IsConnected)
            return Task.FromResult(HealthCheckResult.Unhealthy("MQTT broker disconnected", data: data));

        if (_state.ChannelFillPercent >= _opts.BackpressureHighWatermarkPercent)
            return Task.FromResult(HealthCheckResult.Degraded("Channel near capacity", data: data));

        return Task.FromResult(HealthCheckResult.Healthy(data: data));
    }
}

/// <summary>Shared mutable state updated by the consumer service.</summary>
public sealed class MqttConnectionState
{
    public bool IsConnected { get; set; }
    public int ChannelFillPercent { get; set; }
    public int ActiveWorkers { get; set; }
}
