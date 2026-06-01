namespace MqttClient.Configuration;

public sealed class MqttOptions
{
    public const string Section = "Mqtt";

    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 1883;
    public string ClientId { get; set; } = "mqtt-consumer";
    public string? Username { get; set; }
    public string? Password { get; set; }
    public bool CleanSession { get; set; } = false;
    public int KeepAliveSeconds { get; set; } = 30;
    public int SessionExpiryIntervalSeconds { get; set; } = 3600;

    // Exponential backoff: delay = min(BaseDelay * 2^attempt, MaxDelay) ± 20% jitter
    public int ReconnectBaseDelaySeconds { get; set; } = 2;
    public int ReconnectMaxDelaySeconds { get; set; } = 60;

    // TLS
    public bool UseTls { get; set; } = false;
    public bool AllowUntrustedCertificates { get; set; } = false;
    public string? ClientCertificatePath { get; set; }
    public string? ClientCertificatePassword { get; set; }
}
