using System.Threading.Channels;
using MqttClient.Models;
using Microsoft.Extensions.Logging;

namespace MqttClient.Services;

public sealed class DeadLetterService : IAsyncDisposable
{
    private readonly Channel<DeadLetterMessage> _channel;
    private readonly ILogger<DeadLetterService> _logger;
    private readonly Task _drainTask;
    private readonly CancellationTokenSource _cts = new();

    public DeadLetterService(ILogger<DeadLetterService> logger)
    {
        _logger = logger;
        _channel = Channel.CreateBounded<DeadLetterMessage>(new BoundedChannelOptions(1_000)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true
        });
        _drainTask = DrainAsync(_cts.Token);
    }

    public ValueTask EnqueueAsync(DeadLetterMessage message) =>
        _channel.Writer.WriteAsync(message);

    private async Task DrainAsync(CancellationToken ct)
    {
        await foreach (var msg in _channel.Reader.ReadAllAsync(ct))
        {
            _logger.LogError(
                msg.Exception,
                "Dead-letter: topic={Topic} correlationId={CorrelationId} reason={Reason} retries={Retries} failedAt={FailedAt}",
                msg.OriginalMessage.Topic,
                msg.OriginalMessage.CorrelationId,
                msg.Reason,
                msg.OriginalMessage.RetryCount,
                msg.FailedAt);

            // TODO: persist to external store (e.g., Redis, MQTT topic, file)
        }
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();
        await _cts.CancelAsync();
        try { await _drainTask; } catch (OperationCanceledException) { }
        _cts.Dispose();
    }
}
