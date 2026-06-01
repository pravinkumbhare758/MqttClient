using System.Text;
using MqttClient.Interfaces;
using MqttClient.Models;
using Microsoft.Extensions.Logging;

namespace MqttClient.Handlers;

public sealed class CommandHandler : IMqttMessageHandler
{
    private readonly ILogger<CommandHandler> _logger;

    public string TopicFilter => "devices/+/commands/#";

    public CommandHandler(ILogger<CommandHandler> logger) => _logger = logger;

    public Task HandleAsync(MqttMessage message, CancellationToken cancellationToken)
    {
        var payload = Encoding.UTF8.GetString(message.Payload);
        _logger.LogInformation(
            "Command received: topic={Topic} correlationId={CorrelationId} payload={Payload}",
            message.Topic, message.CorrelationId, payload);

        // TODO: dispatch command to device twin or command bus
        return Task.CompletedTask;
    }
}
