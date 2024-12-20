using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Ling.RabbitMQ;

public interface IWorkQueueService
{
    Task EnqueueAsync<T>(
        string queue,
        T message,
        CancellationToken cancellationToken = default)
        where T : class;

    Task SubscribeAsync<T>(
        string queue,
        Func<T?, CancellationToken, Task> handler,
        ushort prefetchCount = 1,
        bool requeue = false,
        CancellationToken cancellationToken = default)
        where T : class;
}

public class WorkQueueService : RabbitMQServiceBase, IWorkQueueService
{
    public WorkQueueService(RabbitMQConnection connection) : base(connection)
    {
        DefaultProperties.Persistent = true;
    }

    public WorkQueueService(RabbitMQConnection connection, ILoggerFactory loggerFactory) : base(connection, loggerFactory)
    {
        DefaultProperties.Persistent = true;
    }

    public virtual async Task EnqueueAsync<T>(
        string queue,
        T message,
        CancellationToken cancellationToken = default)
        where T : class
    {
        try
        {
            await ConnectAsync(cancellationToken);

            await DeclareQueueAsync(queue, true, null, cancellationToken);

            var body = Serialize(message);
            await Channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queue,
                mandatory: true,
                basicProperties: DefaultProperties,
                body: body,
                cancellationToken: cancellationToken);

            Logger.LogInformation("Message enqueued to {Queue} with message: {@message}", queue, message);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to enqueue message to {Queue} with message: {@message}", queue, message);
            throw;
        }
    }

    public virtual async Task SubscribeAsync<T>(
        string queue,
        Func<T?, CancellationToken, Task> handler,
        ushort prefetchCount = 1,
        bool requeue = false,
        CancellationToken cancellationToken = default)
        where T : class
    {
        try
        {
            await ConnectAsync(cancellationToken);

            await DeclareQueueAsync(queue, true, null, cancellationToken);

            await Channel.BasicQosAsync(0, prefetchCount, false, cancellationToken);

            var consumer = CreateConsumer(handler, requeue);

            await Channel.BasicConsumeAsync(
                queue: queue,
                autoAck: false,
                consumer: consumer,
                cancellationToken: cancellationToken);

            Logger.LogInformation("Subscribed to queue {Queue}", queue);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to subscribe to {Queue}", queue);
            throw;
        }
    }
}
