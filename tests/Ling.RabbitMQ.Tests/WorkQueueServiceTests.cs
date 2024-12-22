using Microsoft.Extensions.Logging;
using Moq;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Tests;

public class WorkQueueServiceTests
{
    private readonly Mock<RabbitMQConnection> _connectionMock;
    private readonly Mock<IChannel> _channelMock;
    private readonly Mock<ILoggerFactory> _loggerMock;
    private readonly WorkQueueService _service;

    public WorkQueueServiceTests()
    {
        _connectionMock = new Mock<RabbitMQConnection>();
        _channelMock = new Mock<IChannel>();
        _loggerMock = new Mock<ILoggerFactory>();

        _channelMock.Setup(x => x.IsOpen).Returns(true);
        _connectionMock.Setup(x => x.CreateChannelAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(_channelMock.Object);

        _service = new WorkQueueService(_connectionMock.Object, _loggerMock.Object);
    }

    [Fact]
    public async Task EnqueueAsync_ShouldPublishMessage()
    {
        // Arrange
        var queue = "test_queue";
        var message = new { Id = 1, Name = "Test" };

        // Act
        await _service.EnqueueAsync(queue, message);

        // Assert
        _channelMock.Verify(x => x.QueueDeclare(
            queue,
            true,
            false,
            false,
            null));

        _channelMock.Verify(x => x.BasicPublish(
            "",
            queue,
            It.IsAny<bool>(),
            It.IsAny<IBasicProperties>(),
            It.IsAny<ReadOnlyMemory<byte>>()));
    }

    [Fact]
    public async Task ConsumeAsync_ShouldSetupConsumer()
    {
        // Arrange
        var queue = "test_queue";
        var handler = Mock.Of<Func<object?, CancellationToken, Task>>();

        // Act
        await _service.ConsumeAsync(queue, handler);

        // Assert
        _channelMock.Verify(x => x.QueueDeclare(
            queue,
            true,
            false,
            false,
            null));

        _channelMock.Verify(x => x.BasicQos(
            0,
            1,
            false));
    }
} 