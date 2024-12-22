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
internal class RabbitMQBuilder : IRabbitMQBuilder
{
    /// <inheritdoc/>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="IRabbitMQBuilder"/> class.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> where RabbitMQ services are configured.</param>
    public RabbitMQBuilder(IServiceCollection services)
    {
        Services = services;

        Services.TryAddSingleton<IMessageSerializer, DefaultMessageSerializer>();
    }

    public IRabbitMQBuilder UseSerializer<TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        Services.Replace(ServiceDescriptor.Singleton<IMessageSerializer, TSerializer>());

        return this;
    }
}
