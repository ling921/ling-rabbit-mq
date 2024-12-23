using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Ling.RabbitMQ;

/// <summary>
/// A builder for configuring RabbitMQ services.
/// </summary>
public interface IRabbitMQBuilder
{
    /// <summary>
    /// Gets the <see cref="IServiceCollection"/> where RabbitMQ services are configured.
    /// </summary>
    IServiceCollection Services { get; }
}

/// <summary>
/// Implementation of <see cref="IRabbitMQBuilder"/> for configuring RabbitMQ services.
/// </summary>
internal sealed class RabbitMQBuilder : IRabbitMQBuilder
{
    /// <inheritdoc/>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMQBuilder"/> class.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> where RabbitMQ services are configured.</param>
    public RabbitMQBuilder(IServiceCollection services)
    {
        Services = services;

        Services.TryAddSingleton<IMessageSerializer, DefaultMessageSerializer>();
    }

    /// <summary>
    /// Adds a custom message serializer to the RabbitMQ services.
    /// </summary>
    /// <typeparam name="TSerializer">The type of the custom message serializer.</typeparam>
    /// <returns>The <see cref="IRabbitMQBuilder"/>.</returns>
    public IRabbitMQBuilder AddSerializer<TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        Services.Replace(ServiceDescriptor.Singleton<IMessageSerializer, TSerializer>());

        return this;
    }
}
