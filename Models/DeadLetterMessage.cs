namespace MqttClient.Models;

public sealed record DeadLetterMessage(
    MqttMessage OriginalMessage,
    string Reason,
    Exception? Exception,
    DateTimeOffset FailedAt);
