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

    // Accepts ActivityContext (a struct) so the caller does not need to keep
    // the parent Activity alive — the context is self-contained after capture.
    public static Activity? StartProcess(string topic, string correlationId, ActivityContext parentContext)
    {
        var activity = Source.StartActivity(
            "mqtt.process",
            ActivityKind.Internal,
            parentContext);
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
