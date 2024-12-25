using Ling.RabbitMQ.Consumers;
using Ling.RabbitMQ.Producers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.IntegrationTests;

public class WorkQueueServiceIntegrationTests : IAsyncLifetime
{
    private static readonly string queueName = $"test_queue_{Guid.NewGuid()}";

    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<WorkQueueServiceIntegrationTests> _logger;
    private readonly IWorkQueueProducer _workQueueProducer;
    private readonly WorkQueueConsumer _workQueueConsumer;

    public WorkQueueServiceIntegrationTests()
    {
        var services = new ServiceCollection();

        services.AddLogging(builder => builder.AddConsole());
        services
            .AddRabbitMQ(options =>
            {
                options.HostName = "localhost";
                options.Port = 5672;
                options.UserName = "guest";
                options.Password = "guest";
            })
            .AddWorkQueueProducer()
            .AddWorkQueueConsumer<WorkQueueConsumer, TestMessage>();

        services.TryAddSingleton(sp =>
        {
            var factory = new ConnectionFactory
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "guest",
                Password = "guest",
            };

            return factory.CreateConnectionAsync().Result;
        });

        _serviceProvider = services.BuildServiceProvider();
        _logger = _serviceProvider.GetRequiredService<ILogger<WorkQueueServiceIntegrationTests>>();
        _workQueueProducer = _serviceProvider.GetRequiredService<IWorkQueueProducer>();
        _workQueueConsumer = _serviceProvider.GetRequiredService<WorkQueueConsumer>();
    }

    [Fact]
    public async Task EnqueueAndSubscribe_ShouldDeliverMessage()
    {
        // Arrange
        var message = new TestMessage { Content = "Test Message" };
        var messageReceived = new TaskCompletionSource<TestMessage?>();

        _workQueueConsumer.MessageReceived += (_, msg) => messageReceived.TrySetResult(msg);

        var tasks = new[]
        {
            _workQueueConsumer.StartAsync(CancellationToken.None),
            _workQueueProducer.InvokeAsync(queueName, message),
        };
        await Task.WhenAll(tasks);

        // Assert
        var receivedMessage = await messageReceived.Task.WaitAsync(TimeSpan.FromSeconds(50));
        Assert.NotNull(receivedMessage);
        Assert.Equal(message.Content, receivedMessage.Content);
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_serviceProvider is IAsyncDisposable asyncDisposable)
        {
            await asyncDisposable.DisposeAsync();
        }
    }

    private class TestMessage
    {
        public string Content { get; set; } = string.Empty;
    }

    private class WorkQueueConsumer : WorkQueueConsumer<TestMessage>
    {
        internal event EventHandler<TestMessage?> MessageReceived = default!;

        protected override string QueueName => queueName;

        public WorkQueueConsumer(
            ILoggerFactory loggerFactory,
            IMessageSerializer serializer,
            IOptions<RabbitMQOptions> options)
            : base(loggerFactory, serializer, options)
        {
        }

        protected override Task ConsumeAsync(TestMessage? message, string routingKey, IReadOnlyBasicProperties basicProperties, CancellationToken cancellationToken)
        {
            MessageReceived?.Invoke(this, message);

            return Task.CompletedTask;
        }
    }
}
