using System.Diagnostics.Metrics;

namespace MqttClient.Metrics;

public sealed class MqttMetrics : IDisposable
{
    public static readonly string MeterName = "MqttClient";

    private readonly Meter _meter;

    public readonly Counter<long> MessagesReceived;
    public readonly Counter<long> MessagesProcessed;
    public readonly Counter<long> MessagesFailed;
    public readonly Counter<long> MessagesDeadLettered;
    public readonly Counter<long> MessagesRetried;
    public readonly UpDownCounter<int> WorkerCount;
    public readonly UpDownCounter<int> ChannelOccupancy;
    public readonly Histogram<double> ProcessingLatencyMs;
    public readonly Histogram<double> EndToEndLatencyMs;

    public MqttMetrics()
    {
        _meter = new Meter(MeterName, "1.0.0");

        MessagesReceived    = _meter.CreateCounter<long>("mqtt.messages.received",    "messages", "Total messages received from broker");
        MessagesProcessed   = _meter.CreateCounter<long>("mqtt.messages.processed",   "messages", "Total messages successfully processed");
        MessagesFailed      = _meter.CreateCounter<long>("mqtt.messages.failed",      "messages", "Total messages that failed processing");
        MessagesDeadLettered= _meter.CreateCounter<long>("mqtt.messages.deadlettered","messages", "Total messages sent to dead-letter channel");
        MessagesRetried     = _meter.CreateCounter<long>("mqtt.messages.retried",     "messages", "Total message retry attempts");
        WorkerCount         = _meter.CreateUpDownCounter<int>("mqtt.workers.active",  "workers",  "Current number of active consumer workers");
        ChannelOccupancy    = _meter.CreateUpDownCounter<int>("mqtt.channel.occupancy","messages","Current messages waiting in channel");
        ProcessingLatencyMs = _meter.CreateHistogram<double>("mqtt.processing.latency_ms", "ms",  "Time from dequeue to handler completion");
        EndToEndLatencyMs   = _meter.CreateHistogram<double>("mqtt.e2e.latency_ms",   "ms",       "Time from receive to handler completion");
    }

    public void Dispose() => _meter.Dispose();
}
