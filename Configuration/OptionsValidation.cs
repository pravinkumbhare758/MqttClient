using Microsoft.Extensions.Options;

namespace MqttClient.Configuration;

public sealed class MqttOptionsValidator : IValidateOptions<MqttOptions>
{
    public ValidateOptionsResult Validate(string? name, MqttOptions o)
    {
        var failures = new List<string>();

        if (string.IsNullOrWhiteSpace(o.Host))
            failures.Add("Mqtt:Host must not be empty");
        if (o.Port is < 1 or > 65535)
            failures.Add("Mqtt:Port must be between 1 and 65535");
        if (string.IsNullOrWhiteSpace(o.ClientId))
            failures.Add("Mqtt:ClientId must not be empty");
        if (o.KeepAliveSeconds < 0)
            failures.Add("Mqtt:KeepAliveSeconds must be >= 0");
        if (o.ReconnectBaseDelaySeconds < 1)
            failures.Add("Mqtt:ReconnectBaseDelaySeconds must be >= 1");
        if (o.ReconnectMaxDelaySeconds < o.ReconnectBaseDelaySeconds)
            failures.Add("Mqtt:ReconnectMaxDelaySeconds must be >= ReconnectBaseDelaySeconds");
        if (o.UseTls && o.AllowUntrustedCertificates)
            failures.Add("Mqtt:AllowUntrustedCertificates=true must not be used in production");

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }
}

public sealed class WorkerPoolOptionsValidator : IValidateOptions<WorkerPoolOptions>
{
    public ValidateOptionsResult Validate(string? name, WorkerPoolOptions o)
    {
        var failures = new List<string>();

        if (o.ChannelCapacity < 1)
            failures.Add("WorkerPool:ChannelCapacity must be >= 1");
        if (o.MinWorkerCount < 1)
            failures.Add("WorkerPool:MinWorkerCount must be >= 1");
        if (o.InitialWorkerCount < o.MinWorkerCount)
            failures.Add("WorkerPool:InitialWorkerCount must be >= MinWorkerCount");
        if (o.MaxWorkerCount < o.InitialWorkerCount)
            failures.Add("WorkerPool:MaxWorkerCount must be >= InitialWorkerCount");
        if (o.ScaleUpThresholdPercent <= o.ScaleDownThresholdPercent)
            failures.Add("WorkerPool:ScaleUpThresholdPercent must be > ScaleDownThresholdPercent");
        if (o.BackpressureHighWatermarkPercent is < 1 or > 100)
            failures.Add("WorkerPool:BackpressureHighWatermarkPercent must be 1–100");
        if (o.ShutdownTimeoutSeconds < 1)
            failures.Add("WorkerPool:ShutdownTimeoutSeconds must be >= 1");
        if (o.MaxRetries < 0)
            failures.Add("WorkerPool:MaxRetries must be >= 0");

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }
}
