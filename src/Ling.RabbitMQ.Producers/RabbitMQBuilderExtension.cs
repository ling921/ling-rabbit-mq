using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Ling.RabbitMQ.Producers;

public static class RabbitMQBuilderExtensions
{
    /// <summary>
    /// Adds work queue services.
    /// </summary>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddWorkQueueProducer(this IRabbitMQBuilder builder)
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddTransient<IWorkQueueProducer, WorkQueueProducer>();

        return builder;
    }

    /// <summary>
    /// Adds work queue services with a custom implementation.
    /// </summary>
    /// <typeparam name="TProducer">The type of the work queue service implementation.</typeparam>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddWorkQueueProducer<TProducer>(this IRabbitMQBuilder builder) where TProducer : class, IWorkQueueProducer
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddTransient<IWorkQueueProducer, TProducer>();

        return builder;
    }

    /// <summary>
    /// Adds direct exchange services.
    /// </summary>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddRoutingProducer(this IRabbitMQBuilder builder)
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddTransient<IRoutingProducer, RoutingProducer>();

        return builder;
    }

    /// <summary>
    /// Adds direct exchange services with a custom implementation.
    /// </summary>
    /// <typeparam name="TProducer">The type of the direct exchange service implementation.</typeparam>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddRoutingProducer<TProducer>(this IRabbitMQBuilder builder) where TProducer : class, IRoutingProducer
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddTransient<IRoutingProducer, TProducer>();

        return builder;
    }

    /// <summary>
    /// Adds topic exchange services.
    /// </summary>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddTopicsProducer(this IRabbitMQBuilder builder)
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddSingleton<ITopicPatternVerifier, DefaultTopicPatternVerifier>();
        builder.Services.TryAddTransient<ITopicsProducer, TopicsProducer>();

        return builder;
    }

    /// <summary>
    /// Adds topic exchange services with a custom implementation.
    /// </summary>
    /// <typeparam name="TProducer">The type of the topic exchange service implementation.</typeparam>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddTopicsProducer<TProducer>(this IRabbitMQBuilder builder) where TProducer : class, ITopicsProducer
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddSingleton<ITopicPatternVerifier, DefaultTopicPatternVerifier>();
        builder.Services.TryAddTransient<ITopicsProducer, TProducer>();

        return builder;
    }

    /// <summary>
    /// Adds topic exchange services with a custom implementation.
    /// </summary>
    /// <typeparam name="TProducer">The type of the topic exchange service implementation.</typeparam>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddTopicsProducer<TProducer, TVerifier>(this IRabbitMQBuilder builder)
        where TProducer : class, ITopicsProducer
        where TVerifier : class, ITopicPatternVerifier
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddSingleton<ITopicPatternVerifier, TVerifier>();
        builder.Services.TryAddTransient<ITopicsProducer, TProducer>();

        return builder;
    }

    /// <summary>
    /// Adds publish-subscribe services.
    /// </summary>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddPubSubProducer(this IRabbitMQBuilder builder)
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddTransient<IPubSubProducer, PubSubProducer>();

        return builder;
    }

    /// <summary>
    /// Adds publish-subscribe services with a custom implementation.
    /// </summary>
    /// <typeparam name="TProducer">The type of the pub-sub service implementation.</typeparam>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public static IRabbitMQBuilder AddPubSubProducer<TProducer>(this IRabbitMQBuilder builder) where TProducer : class, IPubSubProducer
    {
        ThrowHelpers.ThrowIfNull(builder);

        builder.AddProducerCore();
        builder.Services.TryAddTransient<IPubSubProducer, TProducer>();

        return builder;
    }

    private static void AddProducerCore(this IRabbitMQBuilder builder)
    {
        builder.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<RabbitMQOptions>>().Value);
    }
}
