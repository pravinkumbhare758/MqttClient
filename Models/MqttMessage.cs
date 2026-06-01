namespace MqttClient.Models;

public sealed record MqttMessage(
    string Topic,
    byte[] Payload,
    MQTTnet.Protocol.MqttQualityOfServiceLevel QoS,
    bool Retain,
    string CorrelationId,
    DateTimeOffset ReceivedAt,
    System.Diagnostics.Activity? ParentActivity,
    int RetryCount = 0);
