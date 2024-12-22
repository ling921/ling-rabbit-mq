using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Ling.RabbitMQ;

/// <summary>
/// Extension methods for setting up RabbitMQ services in an <see cref="IServiceCollection" />.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds RabbitMQ services to the specified <see cref="IServiceCollection" />.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="configure">The action to configure the RabbitMQ options.</param>
    /// <returns>An <see cref="IRabbitMQBuilder"/> for chaining additional configuration.</returns>
    public static IRabbitMQBuilder AddRabbitMQ(this IServiceCollection services, Action<RabbitMQOptions> configure)
    {
        services.AddOptions<RabbitMQOptions>()
            .Configure(configure)
#if NET8_0_OR_GREATER
            .ValidateDataAnnotations()
            .ValidateOnStart();
#else
            .ValidateDataAnnotations();
#endif

        return new RabbitMQBuilder(services);
    }
}
