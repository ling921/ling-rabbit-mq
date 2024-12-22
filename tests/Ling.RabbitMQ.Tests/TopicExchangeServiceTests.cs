using Microsoft.Extensions.Logging;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;

namespace Ling.RabbitMQ.Tests;

public class TopicExchangeServiceTests
{
    private readonly Mock<RabbitMQConnection> _connectionMock;
    private readonly Mock<IChannel> _channelMock;
    private readonly Mock<ILoggerFactory> _loggerMock;
    private readonly TopicExchangeService _service;

    public TopicExchangeServiceTests()
    {
        _connectionMock = new Mock<RabbitMQConnection>();
        _channelMock = new Mock<IChannel>();
        _loggerMock = new Mock<ILoggerFactory>();

        // 设置基本的模拟行为
        _channelMock.Setup(x => x.IsOpen).Returns(true);
        _connectionMock.Setup(x => x.CreateChannelAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(_channelMock.Object);

        _service = new TopicExchangeService(_connectionMock.Object, _loggerMock.Object);
    }

    [Fact]
    public async Task PublishAsync_WithValidTopic_ShouldPublishMessage()
    {
        // Arrange
        var exchange = "test_exchange";
        var topic = "test.topic";
        var message = new { Id = 1, Name = "Test" };

        // Act
        await _service.PublishAsync(exchange, topic, message);

        // Assert
        _channelMock.Verify(x => x.ExchangeDeclare(
            exchange,
            ExchangeType.Topic,
            true,
            false,
            null));

        _channelMock.Verify(x => x.BasicPublish(
            exchange,
            topic,
            It.IsAny<bool>(),
            It.IsAny<IBasicProperties>(),
            It.IsAny<ReadOnlyMemory<byte>>()));
    }

    [Fact]
    public async Task PublishAsync_WithInvalidTopic_ShouldThrowException()
    {
        // Arrange
        var exchange = "test_exchange";
        var invalidTopic = "test..topic";
        var message = new { Id = 1, Name = "Test" };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() =>
            _service.PublishAsync(exchange, invalidTopic, message));
    }

    [Fact]
    public async Task SubscribeAsync_WithValidPattern_ShouldSetupSubscription()
    {
        // Arrange
        var exchange = "test_exchange";
        var queue = "test_queue";
        var pattern = "test.*";
        var handler = Mock.Of<Func<object?, IReadOnlyBasicProperties, CancellationToken, Task>>();

        // Act
        await _service.SubscribeAsync(exchange, queue, pattern, handler);

        // Assert
        _channelMock.Verify(x => x.ExchangeDeclare(
            exchange,
            ExchangeType.Topic,
            true,
            false,
            null));

        _channelMock.Verify(x => x.QueueDeclare(
            queue,
            true,
            false,
            false,
            null));

        _channelMock.Verify(x => x.QueueBind(
            queue,
            exchange,
            pattern,
            null));

        _channelMock.Verify(x => x.BasicQos(
            0,
            10,
            false));
    }

    [Fact]
    public async Task SubscribeAsync_WhenChannelFails_ShouldRetry()
    {
        // Arrange
        var failingChannelMock = new Mock<IChannel>();
        var attemptCount = 0;

        _connectionMock.Setup(x => x.CreateChannelAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns<string, CancellationToken>((_, _) =>
            {
                attemptCount++;
                if (attemptCount < 2)
                {
                    throw new Exception("Channel creation failed");
                }
                return Task.FromResult(failingChannelMock.Object);
            });

        // Act
        await _service.SubscribeAsync(
            "test_exchange",
            "test_queue",
            "test.*",
            Mock.Of<Func<object?, IReadOnlyBasicProperties, CancellationToken, Task>>());

        // Assert
        Assert.Equal(2, attemptCount);
    }
}
