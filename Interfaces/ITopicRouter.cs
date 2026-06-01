using MqttClient.Interfaces;

namespace MqttClient.Interfaces;

public interface ITopicRouter
{
    IMqttMessageHandler? Resolve(string topic);
    IReadOnlyList<string> RegisteredFilters { get; }
}
