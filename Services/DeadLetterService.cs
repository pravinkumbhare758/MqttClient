using System.Threading.Channels;
using MqttClient.Models;
using Microsoft.Extensions.Logging;

namespace MqttClient.Services;

public sealed class DeadLetterService : IAsyncDisposable
{
    private readonly Channel<DeadLetterMessage> _channel;
    private readonly ILogger<DeadLetterService> _logger;
    private readonly Task _drainTask;

    public DeadLetterService(ILogger<DeadLetterService> logger)
    {
        _logger = logger;
        _channel = Channel.CreateBounded<DeadLetterMessage>(new BoundedChannelOptions(1_000)
        {
            FullMode     = BoundedChannelFullMode.DropOldest,
            SingleReader = true
        });
        _drainTask = DrainAsync();
    }

    public ValueTask EnqueueAsync(DeadLetterMessage message)
    {
        // Writer may be completed after DisposeAsync; swallow the closed-channel exception
        // so callers don't crash during a tight shutdown race.
        if (_channel.Writer.TryWrite(message)) return ValueTask.CompletedTask;
        return _channel.Writer.WriteAsync(message);
    }

    private async Task DrainAsync()
    {
        // ReadAllAsync(CancellationToken.None): drains ALL queued items before exiting,
        // even if the writer is completed. This ensures DLC messages are never lost.
        await foreach (var msg in _channel.Reader.ReadAllAsync(CancellationToken.None))
        {
            _logger.LogError(
                msg.Exception,
                "Dead-letter: topic={Topic} correlationId={CorrelationId} reason={Reason} failedAt={FailedAt}",
                msg.OriginalMessage.Topic,
                msg.OriginalMessage.CorrelationId,
                msg.Reason,
                msg.FailedAt);

            // TODO: persist to external store (e.g., Redis, dedicated MQTT topic, file)
        }
    }

    public async ValueTask DisposeAsync()
    {
        // 1. Signal no more writes — DrainAsync will process remaining items and then exit.
        _channel.Writer.TryComplete();

        // 2. Wait for all items to drain (no timeout: DLC flush is part of graceful shutdown).
        try { await _drainTask.ConfigureAwait(false); }
        catch (OperationCanceledException) { }
    }
}
