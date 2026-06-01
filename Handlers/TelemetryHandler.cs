using System.Text;
using System.Text.Json;
using MqttClient.Interfaces;
using MqttClient.Models;
using Microsoft.Extensions.Logging;

namespace MqttClient.Handlers;

public sealed class TelemetryHandler : IMqttMessageHandler
{
    private readonly ILogger<TelemetryHandler> _logger;

    public string TopicFilter => "devices/+/telemetry";

    public TelemetryHandler(ILogger<TelemetryHandler> logger) => _logger = logger;

    public Task HandleAsync(MqttMessage message, CancellationToken cancellationToken)
    {
        var json = Encoding.UTF8.GetString(message.Payload);
        _logger.LogInformation(
            "Telemetry received: topic={Topic} correlationId={CorrelationId} payload={Payload}",
            message.Topic, message.CorrelationId, json);

        // TODO: parse and persist telemetry payload
        return Task.CompletedTask;
    }
}
