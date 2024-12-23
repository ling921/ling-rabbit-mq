using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Ling.RabbitMQ.Consumers;

/// <summary>
/// Provides extension methods for configuring RabbitMQ consumers.
/// </summary>
public static class RabbitMQBuilderExtensions
{
    /// <summary>
    /// Adds a work queue consumer service.
    /// </summary>
    /// <typeparam name="TConsumer">The type of the work queue consumer implementation.</typeparam>
    /// <typeparam name="TMessage">The type of the message.</typeparam>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddWorkQueueConsumer<TConsumer, TMessage>(this IRabbitMQBuilder builder)
        where TConsumer : WorkQueueConsumer<TMessage>
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.AddConsumer<TConsumer>();

        return builder;
    }

    /// <summary>
    /// Adds a direct exchange consumer service.
    /// </summary>
    /// <typeparam name="TConsumer">The type of the direct exchange consumer implementation.</typeparam>
    /// <typeparam name="TMessage">The type of the message.</typeparam>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddDirectExchangeConsumer<TConsumer, TMessage>(this IRabbitMQBuilder builder)
        where TConsumer : RoutingConsumer<TMessage>
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.AddConsumer<TConsumer>();

        return builder;
    }

    /// <summary>
    /// Adds a topic exchange consumer service.
    /// </summary>
    /// <typeparam name="TConsumer">The type of the topic exchange consumer implementation.</typeparam>
    /// <typeparam name="TMessage">The type of the message.</typeparam>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddTopicExchangeConsumer<TConsumer, TMessage>(this IRabbitMQBuilder builder)
        where TConsumer : TopicsConsumer<TMessage>
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.Services.TryAddSingleton<ITopicPatternVerifier, DefaultTopicPatternVerifier>();
        builder.AddConsumer<TConsumer>();

        return builder;
    }

    /// <summary>
    /// Adds a topic exchange consumer service with a custom topic pattern verifier.
    /// </summary>
    /// <typeparam name="TConsumer">The type of the topic exchange consumer implementation.</typeparam>
    /// <typeparam name="TVerifier">The type of the topic pattern verifier implementation.</typeparam>
    /// <typeparam name="TMessage">The type of the message.</typeparam>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddTopicExchangeConsumer<TConsumer, TVerifier, TMessage>(this IRabbitMQBuilder builder)
        where TConsumer : TopicsConsumer<TMessage>
        where TVerifier : class, ITopicPatternVerifier
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.Services.TryAddSingleton<ITopicPatternVerifier, TVerifier>();
        builder.AddConsumer<TConsumer>();

        return builder;
    }

    /// <summary>
    /// Adds a publish-subscribe consumer service.
    /// </summary>
    /// <typeparam name="TConsumer">The type of the publish-subscribe consumer implementation.</typeparam>
    /// <typeparam name="TMessage">The type of the message.</typeparam>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddPubSubConsumer<TConsumer, TMessage>(this IRabbitMQBuilder builder)
        where TConsumer : PubSubConsumer<TMessage>
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.AddConsumer<TConsumer>();

        return builder;
    }

    /// <summary>
    /// Adds a consumer service to the RabbitMQ builder.
    /// </summary>
    /// <typeparam name="TConsumer">The type of the consumer implementation.</typeparam>
    /// <param name="builder">The RabbitMQ builder.</param>
    private static void AddConsumer<TConsumer>(this IRabbitMQBuilder builder)
        where TConsumer : class, IHostedService
    {
        builder.Services.TryAddSingleton<TConsumer>();
        builder.Services.AddHostedService(sp => sp.GetRequiredService<TConsumer>());
    }
}
