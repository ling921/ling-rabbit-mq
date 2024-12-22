using Ling.RabbitMQ;
using Ling.RabbitMQ.Consumers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

var builder = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        services.AddRabbitMQ(options =>
        {
            options.HostName = "localhost";
            options.Port = 5672;
            options.UserName = "guest";
            options.Password = "guest";
        }).AddWorkQueueConsumer<ConsumerService, TestMessage>();
    });

builder.UseConsoleLifetime();

var app = builder.Build();

await app.RunAsync();

class TestMessage
{
    public string Content { get; set; } = default!;
}

class ConsumerService : WorkQueueConsumer<TestMessage>
{
    public ConsumerService(ILoggerFactory loggerFactory, IMessageSerializer serializer, IOptions<RabbitMQOptions> options) : base(loggerFactory, serializer, options)
    {
    }

    protected override string QueueName => "test_queue";

    protected override Task ConsumeAsync(TestMessage? message, string routingKey, IReadOnlyBasicProperties basicProperties, CancellationToken cancellationToken)
    {
        Console.WriteLine("Received message: {0}", message?.Content);
        return Task.CompletedTask;
    }
}
