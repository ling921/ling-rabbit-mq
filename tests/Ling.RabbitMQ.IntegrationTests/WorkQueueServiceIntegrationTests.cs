using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Ling.RabbitMQ.IntegrationTests;

public class WorkQueueServiceIntegrationTests : IAsyncLifetime
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IWorkQueueService _workQueueService;
    private readonly ILogger<WorkQueueServiceIntegrationTests> _logger;

    public WorkQueueServiceIntegrationTests()
    {
        var services = new ServiceCollection();

        services.AddLogging(builder => builder.AddConsole());
        services.AddRabbitMQ(options =>
        {
            options.HostName = "localhost";
            options.Port = 5672;
            options.UserName = "guest";
            options.Password = "guest";
        });

        _serviceProvider = services.BuildServiceProvider();
        _workQueueService = _serviceProvider.GetRequiredService<IWorkQueueService>();
        _logger = _serviceProvider.GetRequiredService<ILogger<WorkQueueServiceIntegrationTests>>();
    }

    [Fact]
    public async Task EnqueueAndSubscribe_ShouldDeliverMessage()
    {
        // Arrange
        var message = new TestMessage { Content = "Test Message" };
        var messageReceived = new TaskCompletionSource<TestMessage>();
        var queue = $"test_queue_{Guid.NewGuid()}";

        // 先设置订阅者
        await _workQueueService.SubscribeAsync<TestMessage>(
            queue: queue,
            handler: async (msg, props, ct) =>
            {
                try 
                {
                    _logger.LogInformation("Received message: {Content}", msg?.Content);
                    if (msg != null)
                    {
                        messageReceived.TrySetResult(msg);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing message");
                    messageReceived.TrySetException(ex);
                }
            },
            prefetchCount: 1,
            requeue: false);

        // 确保订阅者已经设置完成
        await Task.Delay(1000);

        // 发送消息
        await _workQueueService.EnqueueAsync(queue, message);
        _logger.LogInformation("Message enqueued");

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
}