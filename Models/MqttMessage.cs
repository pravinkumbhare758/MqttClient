using System.Diagnostics;

namespace MqttClient.Models;

public sealed record MqttMessage(
    string Topic,
    byte[] Payload,
    MQTTnet.Protocol.MqttQualityOfServiceLevel QoS,
    bool Retain,
    string CorrelationId,
    DateTimeOffset ReceivedAt,
    // Store the struct (not the Activity object) so the parent span context
    // remains valid after OnMessageReceivedAsync disposes the receive activity.
    ActivityContext ParentContext);
