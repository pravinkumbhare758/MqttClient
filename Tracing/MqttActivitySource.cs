using System.Diagnostics;

namespace MqttClient.Tracing;

public static class MqttActivitySource
{
    public static readonly ActivitySource Source = new("MqttClient", "1.0.0");

    public static Activity? StartReceive(string topic, string correlationId)
    {
        var activity = Source.StartActivity("mqtt.receive", ActivityKind.Consumer);
        activity?.SetTag("messaging.system", "mqtt");
        activity?.SetTag("messaging.destination", topic);
        activity?.SetTag("messaging.correlation_id", correlationId);
        return activity;
    }

    public static Activity? StartProcess(string topic, string correlationId, Activity? parent)
    {
        var activity = Source.StartActivity(
            "mqtt.process",
            ActivityKind.Internal,
            parent?.Context ?? default);
        activity?.SetTag("messaging.system", "mqtt");
        activity?.SetTag("messaging.destination", topic);
        activity?.SetTag("messaging.correlation_id", correlationId);
        return activity;
    }

    public static Activity? StartEnqueue(string topic, string correlationId)
    {
        var activity = Source.StartActivity("mqtt.enqueue", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "mqtt");
        activity?.SetTag("messaging.destination", topic);
        activity?.SetTag("messaging.correlation_id", correlationId);
        return activity;
    }
}
