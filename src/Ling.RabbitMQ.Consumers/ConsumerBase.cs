using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Ling.RabbitMQ.Consumers;

/// <summary>
/// Represents a base class for consumers.
/// </summary>
/// <typeparam name="TMessage">The type of the message.</typeparam>
public abstract class RabbitMQConsumerBase<TMessage> : RabbitMQServiceBase, IHostedService
    where TMessage : class
{
    /// <summary>
    /// Gets a value indicating whether to requeue the message in case of an error.
    /// </summary>
    protected virtual bool RequeueOnError { get; }

    protected virtual bool IsQosEnabled { get; }

    /// <summary>
    /// Gets the size of the prefetch window in octets. Defaults to 0 for no specific limit.
    /// </summary>
    protected virtual ushort PrefetchSize { get; }

    /// <summary>
    /// Gets the number of messages to prefetch. Defaults to 1 for fair dispatch.
    /// </summary>
    protected virtual ushort PrefetchCount { get; } = 1;

    /// <summary>
    /// Gets a value indicating whether the queue is durable. Defaults to true.
    /// </summary>
    protected virtual bool Durable { get; } = true;

    /// <summary>
    /// Gets a value indicating whether the queue is exclusive. Defaults to null.
    /// </summary>
    protected virtual Dictionary<string, object?>? Arguments { get; }

    /// <summary>
    /// Gets a value indicating whether the queue is exclusive. Defaults to true.
    /// </summary>
    protected virtual bool AutoAck { get; } = true;

    protected RabbitMQConsumerBase(
        ILoggerFactory loggerFactory,
        IMessageSerializer serializer,
        IOptions<RabbitMQOptions> options)
        : base(loggerFactory, options.Value, serializer)
    {
    }

    /// <summary>
    /// Starts the consumer.
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        Logger.LogInformation("Starting consumer {Consumer} for message {Message}", GetType().Name, typeof(TMessage));

        return SetupAsync(cancellationToken);
    }

    /// <summary>
    /// Stops the consumer.
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Logger.LogInformation("Stopping consumer {Consumer} for message {Message}", GetType().Name, typeof(TMessage));

        await Connection.DisposeAsync();
    }

    protected abstract Task SetupAsync(CancellationToken cancellationToken);

    protected abstract Task ConsumeAsync(TMessage? message, string routingKey, IReadOnlyBasicProperties basicProperties, CancellationToken cancellationToken);

    protected Task ApplyQosAsync(IChannel channel, CancellationToken cancellationToken)
    {
        return channel.BasicQosAsync(
            prefetchSize: PrefetchSize,
            prefetchCount: PrefetchCount,
            global: false,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Creates an asynchronous eventing basic consumer.
    /// </summary>
    /// <param name="channel"></param>
    /// <returns>The created asynchronous eventing basic consumer.</returns>
    protected AsyncEventingBasicConsumer CreateConsumer(IChannel channel)
    {
        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (_, ea) =>
        {
            try
            {
                var message = Serializer.Deserialize<TMessage>(ea.Body.Span);

                Logger.LogInformation(
                    "Received message from exchange {Exchange}, routing key {RoutingKey}, basic properties {@BasicProperties}, message {@Message}",
                    ea.Exchange,
                    ea.RoutingKey,
                    ea.BasicProperties,
                    message);

                await ConsumeAsync(message, ea.RoutingKey, ea.BasicProperties, ea.CancellationToken);
                await channel.BasicAckAsync(ea.DeliveryTag, false, ea.CancellationToken);
            }
            catch (OperationCanceledException) when (ea.CancellationToken.IsCancellationRequested)
            {
                await channel.BasicNackAsync(ea.DeliveryTag, false, true);
                Logger.LogInformation("Message {DeliveryTag} requeued due to cancellation", ea.DeliveryTag);
            }
            catch (Exception ex)
            {
                Logger.LogError(
                    ex,
                    "Error processing message from exchange {Exchange}, routing key {RoutingKey}, basic properties {@BasicProperties}",
                    ea.Exchange,
                    ea.RoutingKey,
                    ea.BasicProperties);

                await channel.BasicNackAsync(ea.DeliveryTag, false, RequeueOnError, ea.CancellationToken);
            }
        };

        return consumer;
    }
}
