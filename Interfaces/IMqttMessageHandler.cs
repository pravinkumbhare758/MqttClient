using MqttClient.Models;

namespace MqttClient.Interfaces;

public interface IMqttMessageHandler
{
    /// <summary>Topic filter pattern this handler owns (supports MQTT wildcards).</summary>
    string TopicFilter { get; }

    Task HandleAsync(MqttMessage message, CancellationToken cancellationToken);
}
