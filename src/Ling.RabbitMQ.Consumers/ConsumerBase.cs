using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Ling.RabbitMQ.Consumers;

/// <summary>
/// Represents a base class for RabbitMQ consumers.
/// </summary>
/// <typeparam name="TMessage">The type of the message.</typeparam>
public abstract class RabbitMQConsumerBase<TMessage> : RabbitMQServiceBase, IHostedService
{
    /// <summary>
    /// Gets a value indicating whether to requeue the message in case of an error.
    /// </summary>
    protected virtual bool RequeueOnError { get; }

    /// <summary>
    /// Gets a value indicating whether Quality of Service (QoS) is enabled.
    /// </summary>
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
    /// Gets the arguments for the queue. Defaults to null.
    /// </summary>
    protected virtual Dictionary<string, object?>? Arguments { get; }

    /// <summary>
    /// Gets a value indicating whether the message acknowledgment is automatic. Defaults to true.
    /// </summary>
    protected virtual bool AutoAck { get; } = true;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMQConsumerBase{TMessage}"/> class.
    /// </summary>
    /// <param name="loggerFactory">The logger factory.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="options">The RabbitMQ options.</param>
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
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous start operation.</returns>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        Logger.LogInformation("Starting consumer {ConsumerType} for message type {MessageType}", GetType().Name, typeof(TMessage).Name);

        return SetupAsync(cancellationToken);
    }

    /// <summary>
    /// Stops the consumer.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous stop operation.</returns>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Logger.LogInformation("Stopping consumer {ConsumerType} for message type {MessageType}", GetType().Name, typeof(TMessage).Name);

        await Connection.DisposeAsync();
    }

    /// <summary>
    /// Sets up the consumer asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous setup operation.</returns>
    protected abstract Task SetupAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Consumes the message asynchronously.
    /// </summary>
    /// <param name="message">The message to consume.</param>
    /// <param name="routingKey">The routing key.</param>
    /// <param name="basicProperties">The basic properties of the message.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous consume operation.</returns>
    protected abstract Task ConsumeAsync(TMessage? message, string routingKey, IReadOnlyBasicProperties basicProperties, CancellationToken cancellationToken);

    /// <summary>
    /// Applies Quality of Service (QoS) settings to the channel asynchronously.
    /// </summary>
    /// <param name="channel">The channel.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous QoS application operation.</returns>
    protected async Task ApplyQosAsync(IChannel channel, CancellationToken cancellationToken)
    {
        if (IsQosEnabled)
        {
            await channel.BasicQosAsync(
                prefetchSize: PrefetchSize,
                prefetchCount: PrefetchCount,
                global: false,
                cancellationToken: cancellationToken);
        }
    }

    /// <summary>
    /// Creates an asynchronous eventing basic consumer.
    /// </summary>
    /// <param name="channel">The channel.</param>
    /// <returns>The created asynchronous eventing basic consumer.</returns>
    protected AsyncEventingBasicConsumer CreateConsumer(IChannel channel)
    {
        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (_, ea) =>
        {
            try
            {
                var message = Serializer.Deserialize<TMessage>(ea.Body.Span);

                Logger.LogDebug(
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
                Logger.LogWarning("Message {DeliveryTag} requeued due to cancellation", ea.DeliveryTag);
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
