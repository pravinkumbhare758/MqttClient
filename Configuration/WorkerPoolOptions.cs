namespace MqttClient.Configuration;

public sealed class WorkerPoolOptions
{
    public const string Section = "WorkerPool";

    public int InitialWorkerCount { get; set; } = 4;
    public int MaxWorkerCount { get; set; } = 16;
    public int MinWorkerCount { get; set; } = 1;
    public int ChannelCapacity { get; set; } = 10_000;
    public int ScaleUpThresholdPercent { get; set; } = 80;
    public int ScaleDownThresholdPercent { get; set; } = 20;
    public int ScaleCheckIntervalMs { get; set; } = 5_000;
    public int MaxRetries { get; set; } = 3;
    public int ShutdownTimeoutSeconds { get; set; } = 30;
    public int CircuitBreakerFailureThreshold { get; set; } = 5;
    public int CircuitBreakerDurationSeconds { get; set; } = 30;
    public int BackpressureHighWatermarkPercent { get; set; } = 80;
}
