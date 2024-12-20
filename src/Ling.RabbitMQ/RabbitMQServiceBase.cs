using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Threading.Channels;

namespace Ling.RabbitMQ;

/// <summary>
/// Base class for RabbitMQ services, providing common functionality for connecting to RabbitMQ,
/// declaring exchanges and queues, and serializing/deserializing messages.
/// </summary>
public abstract class RabbitMQServiceBase : IAsyncDisposable
{
#if NET9_0_OR_GREATER
    private static readonly JsonSerializerOptions _jsonerializerOptions = JsonSerializerOptions.Web;
#else
    private static readonly JsonSerializerOptions _jsonerializerOptions = new(JsonSerializerDefaults.Web);
#endif

    private bool _disposed;

    /// <summary>
    /// Gets the logger instance.
    /// </summary>
    protected ILogger Logger { get; }

    /// <summary>
    /// Gets the RabbitMQ connection instance.
    /// </summary>
    protected RabbitMQConnection Connection { get; }

    /// <summary>
    /// Gets the RabbitMQ channel instance.
    /// </summary>
    protected IChannel Channel { get; private set; } = default!;

    /// <summary>
    /// Gets the retry policy for connection attempts.
    /// </summary>
    protected virtual RetryPolicy RetryPolicy { get; }

    /// <summary>
    /// Gets the default properties for messages.
    /// </summary>
    protected virtual BasicProperties DefaultProperties => new()
    {
        Persistent = true,
        DeliveryMode = DeliveryModes.Persistent,
        MessageId = Guid.NewGuid().ToString(),
        Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
    };

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMQServiceBase"/> class with the specified connection.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection.</param>
    protected RabbitMQServiceBase(RabbitMQConnection connection)
    {
        Logger = NullLogger.Instance;
        Connection = connection;
        RetryPolicy = Connection.Configuration.RetryPolicy;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMQServiceBase"/> class with the specified
    /// connection and logger factory.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    protected RabbitMQServiceBase(RabbitMQConnection connection, ILoggerFactory loggerFactory)
    {
        Logger = loggerFactory.CreateLogger(GetType());
        Connection = connection;
        RetryPolicy = Connection.Configuration.RetryPolicy;
    }

    /// <summary>
    /// Connects to RabbitMQ and initializes the channel.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous connect operation.</returns>
    [MemberNotNull(nameof(Channel))]
    protected async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (Channel?.IsOpen == true) return;

#pragma warning disable CS8774 // Member must have a non-null value when exiting.

        var attempt = 0;
        var delay = RetryPolicy.InitialInterval;

        while (true)
        {
            try
            {
                Channel = await Connection.CreateChannelAsync(null, cancellationToken);

                await OnConnectedAsync(cancellationToken);

                return;
            }
            catch (Exception ex) when (attempt < RetryPolicy.MaxRetries)
            {
                attempt++;
                Logger.LogWarning(ex,
                    "Connection attempt {Attempt} failed. Retrying in {Delay} seconds...",
                    attempt, delay.TotalSeconds);

                await Task.Delay(delay, cancellationToken);
                delay = TimeSpan.FromTicks(Math.Min(
                    delay.Ticks * RetryPolicy.BackoffMultiplier,
                    RetryPolicy.MaxInterval.Ticks));
            }
        }

#pragma warning restore CS8774 // Member must have a non-null value when exiting.
    }

    /// <summary>
    /// Called when the connection is established. Can be overridden in derived classes to perform
    /// additional actions.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    protected virtual Task OnConnectedAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <summary>
    /// Asynchronously declares an exchange.
    /// </summary>
    /// <param name="exchange">The exchange name.</param>
    /// <param name="type">The exchange type.</param>
    /// <param name="durable">Whether the exchange is durable.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous declare operation.</returns>
    protected async Task DeclareExchangeAsync(
        string exchange,
        string type,
        bool durable = true,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(exchange)) return;

        try
        {
            await Channel.ExchangeDeclareAsync(
                exchange: exchange,
                type: type,
                durable: durable,
                autoDelete: false,
                arguments: null,
                cancellationToken: cancellationToken);

            Logger.LogInformation("Exchange {Exchange} declared successfully", exchange);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to declare exchange {Exchange}", exchange);
            throw;
        }
    }

    /// <summary>
    /// Asynchronously declares a queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="durable">Whether the queue is durable.</param>
    /// <param name="arguments">Additional arguments for the queue declaration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous declare operation.</returns>
    protected async Task DeclareQueueAsync(
        string queue,
        bool durable = true,
        Dictionary<string, object?>? arguments = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(queue)) return;

        try
        {
            await Channel.QueueDeclareAsync(
                queue: queue,
                durable: durable,
                exclusive: false,
                autoDelete: false,
                arguments: arguments,
                cancellationToken: cancellationToken);

            Logger.LogInformation("Queue {Queue} declared successfully", queue);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to declare queue {Queue}", queue);
            throw;
        }
    }

    /// <summary>
    /// Creates an asynchronous eventing basic consumer.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="handler">The handler to process the message.</param>
    /// <param name="requeue">Whether to requeue the message in case of an error.</param>
    /// <returns>The created asynchronous eventing basic consumer.</returns>
    protected AsyncEventingBasicConsumer CreateConsumer<T>(
        Func<T?, CancellationToken, Task> handler,
        bool requeue)
        where T : class
    {
        var consumer = new AsyncEventingBasicConsumer(Channel);

        consumer.ReceivedAsync += async (_, ea) =>
        {
            try
            {
                var message = Deserialize<T>(ea.Body);

                Logger.LogInformation(
                    "Received message from exchange {Exchange}, routing key {RoutingKey}, basic properties {@BasicProperties}, message {message}",
                    ea.Exchange,
                    ea.RoutingKey,
                    ea.BasicProperties,
                    message);

                await handler(message, ea.CancellationToken);

                await Channel.BasicAckAsync(ea.DeliveryTag, false, ea.CancellationToken);
            }
            catch (Exception ex)
            {
                Logger.LogError(
                    ex,
                    "Error processing message from exchange {Exchange}, routing key {RoutingKey}, basic properties {@BasicProperties}",
                    ea.Exchange,
                    ea.RoutingKey,
                    ea.BasicProperties);

                await Channel.BasicNackAsync(ea.DeliveryTag, false, requeue, ea.CancellationToken);
            }
        };

        return consumer;
    }

    /// <summary>
    /// Serializes a message to a byte array.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="message">The message to serialize.</param>
    /// <returns>The serialized message as a byte array.</returns>
    protected virtual byte[] Serialize<T>(T message)
    {
        return JsonSerializer.SerializeToUtf8Bytes(message, _jsonerializerOptions);
    }

    /// <summary>
    /// Deserializes a message from a byte array.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="body">The byte array containing the message.</param>
    /// <returns>The deserialized message.</returns>
    [return: MaybeNull]
    protected virtual T? Deserialize<T>(ReadOnlyMemory<byte> body)
    {
        return JsonSerializer.Deserialize<T>(body.Span, _jsonerializerOptions);
    }

    /// <summary>
    /// Disposes the RabbitMQ connection and channel asynchronously.
    /// </summary>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    public virtual async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        try
        {
            if (Connection is not null)
            {
                await Connection.DisposeAsync();
            }
            if (Channel is not null)
            {
                await Channel.DisposeAsync();
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error disposing RabbitMQ connection");
        }
        finally
        {
            _disposed = true;
        }

        GC.SuppressFinalize(this);
    }
}
