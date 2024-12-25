using Microsoft.Extensions.Options;

namespace Ling.RabbitMQ;

/// <summary>
/// Represents the configuration options for RabbitMQ.
/// </summary>
public class RabbitMQOptions
{
    /// <summary>
    /// Gets or sets the RabbitMQ host name. Defaults to "localhost".
    /// </summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the RabbitMQ port. Defaults to 5672.
    /// </summary>
    public int Port { get; set; } = 5672;

    /// <summary>
    /// Gets or sets the RabbitMQ virtual host. Defaults to "/".
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// Gets or sets the user name for RabbitMQ authentication. Defaults to "guest".
    /// </summary>
    public string UserName { get; set; } = "guest";

    /// <summary>
    /// Gets or sets the password for RabbitMQ authentication. Defaults to "guest".
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    /// Gets or sets the requested heartbeat interval in seconds. 0 means disable heartbeats. Defaults to 60 seconds.
    /// </summary>
    public int RequestedHeartbeat { get; set; } = 60;

    /// <summary>
    /// Gets or sets a value indicating whether to automatically recover connections. Defaults to <see langword="true"/>.
    /// </summary>
    public bool AutomaticRecoveryEnabled { get; set; } = true;

    /// <summary>
    /// Gets or sets the network recovery interval in seconds. Defaults to 5 seconds.
    /// </summary>
    public int NetworkRecoveryInterval { get; set; } = 5;

    /// <summary>
    /// Gets or sets the consumer dispatch concurrency. Defaults to 1.
    /// </summary>
    public ushort ConsumerDispatchConcurrency { get; set; } = 1;
}

/// <summary>
/// Validates the <see cref="RabbitMQOptions"/>.
/// </summary>
internal sealed class RabbitMQOptionsValidator : IValidateOptions<RabbitMQOptions>
{
    /// <summary>
    /// Validates the specified <see cref="RabbitMQOptions"/>.
    /// </summary>
    /// <param name="name">The name of the options instance being validated.</param>
    /// <param name="options">The options instance to validate.</param>
    /// <returns>The result of the validation.</returns>
    public ValidateOptionsResult Validate(string? name, RabbitMQOptions options)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(options.HostName))
        {
            errors.Add("HostName is required");
        }

        if (options.Port is < 1 or > 65535)
        {
            errors.Add("Port must be between 1 and 65535");
        }

        if (string.IsNullOrWhiteSpace(options.VirtualHost))
        {
            errors.Add("VirtualHost is required");
        }

        if (string.IsNullOrWhiteSpace(options.UserName))
        {
            errors.Add("UserName is required");
        }

        if (string.IsNullOrWhiteSpace(options.Password))
        {
            errors.Add("Password is required");
        }

        if (options.RequestedHeartbeat < 0)
        {
            errors.Add("RequestedHeartbeat must be greater than or equal to 0");
        }

        if (options.NetworkRecoveryInterval < 0)
        {
            errors.Add("NetworkRecoveryInterval must be greater than or equal to 0");
        }

        if (options.ConsumerDispatchConcurrency is < 1 or > ushort.MaxValue)
        {
            errors.Add("ConsumerDispatchConcurrency must be between 1 and 65535");
        }

        return errors.Count > 0 ? ValidateOptionsResult.Fail(errors) : ValidateOptionsResult.Success;
    }
}
