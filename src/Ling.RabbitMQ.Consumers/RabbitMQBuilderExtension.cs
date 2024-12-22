using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Ling.RabbitMQ.Consumers;

public static class RabbitMQBuilderExtensions
{
    /// <summary>
    /// Adds work queue services.
    /// </summary>
    /// <typeparam name="TConsumer">The type of the work queue service implementation.</typeparam>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddWorkQueueConsumer<TConsumer, TMessage>(this IRabbitMQBuilder builder)
        where TConsumer : WorkQueueConsumer<TMessage>
        where TMessage : class
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.AddComsumer<TConsumer>();

        return builder;
    }

    /// <summary>
    /// Adds direct exchange services.
    /// </summary>
    /// <typeparam name="TConsumer">The type of the direct exchange service implementation.</typeparam>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddDirectExchangeConsumer<TConsumer, TMessage>(this IRabbitMQBuilder builder)
        where TConsumer : RoutingConsumer<TMessage>
        where TMessage : class
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.AddComsumer<TConsumer>();

        return builder;
    }

    /// <summary>
    /// Adds topic exchange services.
    /// </summary>
    /// <typeparam name="TConsumer">The type of the topic exchange service implementation.</typeparam>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddTopicExchangeConsumer<TConsumer, TMessage>(this IRabbitMQBuilder builder)
        where TConsumer : TopicsConsumer<TMessage>
        where TMessage : class
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.Services.TryAddSingleton<ITopicPatternVerifier, DefaultTopicPatternVerifier>();
        builder.AddComsumer<TConsumer>();

        return builder;
    }

    /// <summary>
    /// Adds topic exchange services with a custom implementation.
    /// </summary>
    /// <typeparam name="TConsumer">The type of the topic exchange service implementation.</typeparam>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddTopicExchangeConsumer<TConsumer, TVerifier, TMessage>(this IRabbitMQBuilder builder)
        where TConsumer : TopicsConsumer<TMessage>
        where TVerifier : class, ITopicPatternVerifier
        where TMessage : class
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.Services.TryAddSingleton<ITopicPatternVerifier, TVerifier>();
        builder.AddComsumer<TConsumer>();

        return builder;
    }

    /// <summary>
    /// Adds publish-subscribe services.
    /// </summary>
    /// <typeparam name="TConsumer">The type of the pub-sub service implementation.</typeparam>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddPubSubConsumer<TConsumer, TMessage>(this IRabbitMQBuilder builder)
        where TConsumer : PubSubConsumer<TMessage>
        where TMessage : class
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.AddComsumer<TConsumer>();

        return builder;
    }

    private static void AddComsumer<TConsumer>(this IRabbitMQBuilder builder)
        where TConsumer : class, IHostedService
    {
        builder.Services.TryAddSingleton<TConsumer>();
        builder.Services.AddHostedService(sp => sp.GetRequiredService<TConsumer>());
    }
}
