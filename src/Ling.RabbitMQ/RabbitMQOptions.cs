using System.ComponentModel.DataAnnotations;

namespace Ling.RabbitMQ;

/// <summary>
/// Represents a RabbitMQ configuration.
/// </summary>
public class RabbitMQOptions
{
    /// <summary>
    /// Gets or sets the RabbitMQ host name.
    /// </summary>
    [Required(ErrorMessage = "HostName is required")]
    public string HostName { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the RabbitMQ port.
    /// </summary>
    [Range(1, 65535, ErrorMessage = "Port must be between 1 and 65535")]
    public int Port { get; set; } = 5672;

    /// <summary>
    /// Gets or sets the RabbitMQ virtual host.
    /// </summary>
    [Required(ErrorMessage = "VirtualHost is required")]
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// Gets or sets the user name.
    /// </summary>
    [Required(ErrorMessage = "UserName is required")]
    public string UserName { get; set; } = "guest";
    /// <summary>
    /// Gets or sets the password.
    /// </summary>
    [Required(ErrorMessage = "Password is required")]
    public string Password { get; set; } = "guest";

    /// <summary>
    /// Gets or sets the requested heartbeat interval in seconds. 0 means disable heartbeats.
    /// </summary>
    [Range(0, int.MaxValue, ErrorMessage = "RequestedHeartbeat must be greater than or equal to 0")]
    public int RequestedHeartbeat { get; set; } = 60;

    /// <summary>
    /// Gets or sets a value indicating whether to automatically recover connections.
    /// </summary>
    public bool AutomaticRecoveryEnabled { get; set; } = true;

    /// <summary>
    /// Gets or sets the network recovery interval in seconds.
    /// </summary>
    [Range(0, int.MaxValue, ErrorMessage = "NetworkRecoveryInterval must be greater than or equal to 0")]
    public int NetworkRecoveryInterval { get; set; } = 5;

    /// <summary>
    /// Gets or sets the consumer dispatch concurrency.
    /// </summary>
    [Range(1, ushort.MaxValue)]
    public ushort ConsumerDispatchConcurrency { get; set; } = 1;

    /// <summary>
    /// Gets or sets the retry policy.
    /// </summary>
    public RetryPolicy RetryPolicy { get; set; } = RetryPolicy.Default;
}

/// <summary>
/// Represents a retry policy configuration.
/// </summary>
public class RetryPolicy
{
    /// <summary>
    /// Gets the default retry policy.
    /// </summary>
    public static RetryPolicy Default => new()
    {
        MaxAttempts = 3,
        DelaySeconds = 1,
        MaxDelaySeconds = 30,
        Multiplier = 2
    };

    /// <summary>
    /// Gets or sets the maximum number of retry attempts.
    /// </summary>
    public int MaxAttempts { get; set; }

    /// <summary>
    /// Gets or sets the initial delay in seconds between retries.
    /// </summary>
    public int DelaySeconds { get; set; }

    /// <summary>
    /// Gets or sets the maximum delay in seconds between retries.
    /// </summary>
    public int MaxDelaySeconds { get; set; }

    /// <summary>
    /// Gets or sets the multiplier for exponential backoff.
    /// </summary>
    public int Multiplier { get; set; }
}
