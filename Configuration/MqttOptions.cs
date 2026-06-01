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
    public int ReconnectDelaySeconds { get; set; } = 5;
    public int SessionExpiryIntervalSeconds { get; set; } = 3600;
    public List<string> Topics { get; set; } = [];
}
