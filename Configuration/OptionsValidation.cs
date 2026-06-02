using Microsoft.Extensions.Options;

namespace MqttClient.Configuration;

public sealed class MqttOptionsValidator : IValidateOptions<MqttOptions>
{
    public ValidateOptionsResult Validate(string? name, MqttOptions o)
    {
        // IValidateOptions requires an eager Success/Fail decision, so we
        // materialize the rule sequence once and branch on the count.
        var failures = CollectFailures(o).ToList();
        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }

    // Each rule reads as a single line; yield drops the repeated failures.Add(...).
    private static IEnumerable<string> CollectFailures(MqttOptions o)
    {
        if (string.IsNullOrWhiteSpace(o.Host))
            yield return "Mqtt:Host must not be empty";
        if (o.Port is < 1 or > 65535)
            yield return "Mqtt:Port must be between 1 and 65535";
        if (string.IsNullOrWhiteSpace(o.ClientId))
            yield return "Mqtt:ClientId must not be empty";
        if (o.KeepAliveSeconds < 0)
            yield return "Mqtt:KeepAliveSeconds must be >= 0";
        if (o.ReconnectBaseDelaySeconds < 1)
            yield return "Mqtt:ReconnectBaseDelaySeconds must be >= 1";
        if (o.ReconnectMaxDelaySeconds < o.ReconnectBaseDelaySeconds)
            yield return "Mqtt:ReconnectMaxDelaySeconds must be >= ReconnectBaseDelaySeconds";
        // AllowUntrustedCertificates is intentionally NOT validated here.
        // Development environments use self-signed certs; enforcement is done
        // via appsettings.Production.json which forces AllowUntrustedCertificates=false.
    }
}

public sealed class WorkerPoolOptionsValidator : IValidateOptions<WorkerPoolOptions>
{
    public ValidateOptionsResult Validate(string? name, WorkerPoolOptions o)
    {
        var failures = CollectFailures(o).ToList();
        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }

    private static IEnumerable<string> CollectFailures(WorkerPoolOptions o)
    {
        if (o.ChannelCapacity < 1)
            yield return "WorkerPool:ChannelCapacity must be >= 1";
        if (o.MinWorkerCount < 1)
            yield return "WorkerPool:MinWorkerCount must be >= 1";
        if (o.InitialWorkerCount < o.MinWorkerCount)
            yield return "WorkerPool:InitialWorkerCount must be >= MinWorkerCount";
        if (o.MaxWorkerCount < o.InitialWorkerCount)
            yield return "WorkerPool:MaxWorkerCount must be >= InitialWorkerCount";
        if (o.ScaleUpThresholdPercent <= o.ScaleDownThresholdPercent)
            yield return "WorkerPool:ScaleUpThresholdPercent must be > ScaleDownThresholdPercent";
        if (o.BackpressureHighWatermarkPercent is < 1 or > 100)
            yield return "WorkerPool:BackpressureHighWatermarkPercent must be 1–100";
        if (o.ShutdownTimeoutSeconds < 1)
            yield return "WorkerPool:ShutdownTimeoutSeconds must be >= 1";
        if (o.MaxRetries < 0)
            yield return "WorkerPool:MaxRetries must be >= 0";
    }
}
