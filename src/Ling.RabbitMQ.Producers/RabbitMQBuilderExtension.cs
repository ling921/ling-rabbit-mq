using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Ling.RabbitMQ.Producers;

/// <summary>
/// Provides extension methods for configuring RabbitMQ producers.
/// </summary>
public static class RabbitMQBuilderExtensions
{
    /// <summary>
    /// Adds work queue producer services.
    /// </summary>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddWorkQueueProducer(this IRabbitMQBuilder builder)
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddScoped<IWorkQueueProducer, WorkQueueProducer>();

        return builder;
    }

    /// <summary>
    /// Adds work queue producer services with a custom implementation.
    /// </summary>
    /// <typeparam name="TProducer">The type of the work queue producer implementation.</typeparam>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddWorkQueueProducer<TProducer>(this IRabbitMQBuilder builder) where TProducer : class, IWorkQueueProducer
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddScoped<IWorkQueueProducer, TProducer>();

        return builder;
    }

    /// <summary>
    /// Adds routing producer services.
    /// </summary>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddRoutingProducer(this IRabbitMQBuilder builder)
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddScoped<IRoutingProducer, RoutingProducer>();

        return builder;
    }

    /// <summary>
    /// Adds routing producer services with a custom implementation.
    /// </summary>
    /// <typeparam name="TProducer">The type of the routing producer implementation.</typeparam>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddRoutingProducer<TProducer>(this IRabbitMQBuilder builder) where TProducer : class, IRoutingProducer
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddScoped<IRoutingProducer, TProducer>();

        return builder;
    }

    /// <summary>
    /// Adds topic exchange producer services.
    /// </summary>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddTopicsProducer(this IRabbitMQBuilder builder)
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddSingleton<ITopicPatternVerifier, DefaultTopicPatternVerifier>();
        builder.Services.TryAddScoped<ITopicsProducer, TopicsProducer>();

        return builder;
    }

    /// <summary>
    /// Adds topic exchange producer services with a custom implementation.
    /// </summary>
    /// <typeparam name="TProducer">The type of the topic exchange producer implementation.</typeparam>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddTopicsProducer<TProducer>(this IRabbitMQBuilder builder) where TProducer : class, ITopicsProducer
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddSingleton<ITopicPatternVerifier, DefaultTopicPatternVerifier>();
        builder.Services.TryAddScoped<ITopicsProducer, TProducer>();

        return builder;
    }

    /// <summary>
    /// Adds topic exchange producer services with a custom implementation and a custom topic pattern verifier.
    /// </summary>
    /// <typeparam name="TProducer">The type of the topic exchange producer implementation.</typeparam>
    /// <typeparam name="TVerifier">The type of the topic pattern verifier implementation.</typeparam>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddTopicsProducer<TProducer, TVerifier>(this IRabbitMQBuilder builder)
        where TProducer : class, ITopicsProducer
        where TVerifier : class, ITopicPatternVerifier
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddSingleton<ITopicPatternVerifier, TVerifier>();
        builder.Services.TryAddScoped<ITopicsProducer, TProducer>();

        return builder;
    }

    /// <summary>
    /// Adds publish-subscribe producer services.
    /// </summary>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddPubSubProducer(this IRabbitMQBuilder builder)
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddScoped<IPubSubProducer, PubSubProducer>();

        return builder;
    }

    /// <summary>
    /// Adds publish-subscribe producer services with a custom implementation.
    /// </summary>
    /// <typeparam name="TProducer">The type of the pub-sub producer implementation.</typeparam>
    /// <param name="builder">The RabbitMQ builder.</param>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddPubSubProducer<TProducer>(this IRabbitMQBuilder builder) where TProducer : class, IPubSubProducer
    {
        ThrowHelper.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddScoped<IPubSubProducer, TProducer>();

        return builder;
    }

    /// <summary>
    /// Adds core services required for RabbitMQ producers.
    /// </summary>
    /// <param name="builder">The RabbitMQ builder.</param>
    private static void AddProducerCore(this IRabbitMQBuilder builder)
    {
        builder.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<RabbitMQOptions>>().Value);
    }
}
