namespace Ling.RabbitMQ;

/// <summary>
/// Represents a RabbitMQ configuration.
/// </summary>
public class RabbitMQConfig
{
    /// <summary>
    /// Gets or sets the RabbitMQ host name.
    /// </summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the RabbitMQ port.
    /// </summary>
    public int Port { get; set; } = 5672;

    /// <summary>
    /// Gets or sets the RabbitMQ user name.
    /// </summary>
    public string UserName { get; set; } = default!;

    /// <summary>
    /// Gets or sets the RabbitMQ password.
    /// </summary>
    public string Password { get; set; } = default!;

    /// <summary>
    /// Gets or sets the RabbitMQ virtual host.
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// Gets or sets a value indicating whether automatic recovery is enabled.
    /// </summary>
    public bool AutomaticRecoveryEnabled { get; set; } = true;

    /// <summary>
    /// Gets or sets the requested heartbeat interval.
    /// </summary>
    public TimeSpan RequestedHeartbeat { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the consumer dispatch concurrency.
    /// </summary>
    public ushort ConsumerDispatchConcurrency { get; set; }

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
    /// Gets or sets a value indicating whether retry policy is enabled.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of retries.
    /// </summary>
    public int MaxRetries { get; set; }

    /// <summary>
    /// Gets or sets the initial interval between retries.
    /// </summary>
    public TimeSpan InitialInterval { get; set; }

    /// <summary>
    /// Gets or sets the maximum interval between retries.
    /// </summary>
    public TimeSpan MaxInterval { get; set; }

    /// <summary>
    /// Gets or sets the backoff multiplier for retries.
    /// </summary>
    public int BackoffMultiplier { get; set; }

    /// <summary>
    /// Gets the default retry policy.
    /// </summary>
    public static RetryPolicy Default => new()
    {
        Enabled = false,
        MaxRetries = 3,
        InitialInterval = TimeSpan.FromSeconds(1),
        MaxInterval = TimeSpan.FromSeconds(30),
        BackoffMultiplier = 2
    };
}
