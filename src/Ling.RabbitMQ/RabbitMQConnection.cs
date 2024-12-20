using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;

namespace Ling.RabbitMQ;

/// <summary>
/// Manages a RabbitMQ connection, ensuring thread-safe access and automatic reconnection.
/// </summary>
public sealed class RabbitMQConnection : IAsyncDisposable
{
    private readonly ReaderWriterLockSlim _lock = new();
    private readonly ILogger _logger;
    private IConnection _connection = default!;
    private bool _disposed;

    /// <summary>
    /// Gets the RabbitMQ configuration.
    /// </summary>
    public RabbitMQConfig Configuration { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMQConnection"/> class with the specified configuration.
    /// </summary>
    /// <param name="config">The RabbitMQ configuration.</param>
    public RabbitMQConnection(RabbitMQConfig config)
    {
        Configuration = config;
        _logger = NullLogger.Instance;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMQConnection"/> class with the specified configuration and logger factory.
    /// </summary>
    /// <param name="config">The RabbitMQ configuration.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public RabbitMQConnection(RabbitMQConfig config, ILoggerFactory loggerFactory)
    {
        Configuration = config;
        _logger = loggerFactory.CreateLogger<RabbitMQConnection>();
    }

    /// <summary>
    /// Gets a value indicating whether the connection is open and not disposed.
    /// </summary>
    public bool IsConnected
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _connection?.IsOpen == true && !_disposed;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Gets the RabbitMQ connection asynchronously, ensuring it is established.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The RabbitMQ connection.</returns>
    public async ValueTask<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        if (IsConnected) return _connection;

        await EnsureConnectionAsync(cancellationToken);

        return _connection;
    }

    /// <summary>
    /// Creates a RabbitMQ channel asynchronously.
    /// </summary>
    /// <param name="options">The channel options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The RabbitMQ channel.</returns>
    public async ValueTask<IChannel> CreateChannelAsync(CreateChannelOptions? options = default, CancellationToken cancellationToken = default)
    {
        await EnsureConnectionAsync(cancellationToken);

        return await _connection.CreateChannelAsync(options, cancellationToken);
    }

    /// <summary>
    /// Ensures the RabbitMQ connection is established asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async ValueTask EnsureConnectionAsync(CancellationToken cancellationToken = default)
    {
        if (IsConnected) return;

        _lock.EnterWriteLock();
        try
        {
            if (IsConnected) return;

            var factory = new ConnectionFactory
            {
                HostName = Configuration.HostName,
                UserName = Configuration.UserName,
                Password = Configuration.Password,
                Port = Configuration.Port,
                VirtualHost = Configuration.VirtualHost,
                AutomaticRecoveryEnabled = Configuration.AutomaticRecoveryEnabled,
                RequestedHeartbeat = Configuration.RequestedHeartbeat,
                ConsumerDispatchConcurrency = Configuration.ConsumerDispatchConcurrency,
            };

            _connection = await factory.CreateConnectionAsync(cancellationToken);

            _logger.LogInformation("RabbitMQ connection established successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to establish RabbitMQ connection");
            throw;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Disposes the RabbitMQ connection asynchronously.
    /// </summary>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        _lock.EnterWriteLock();
        try
        {
            if (_disposed) return;
            _disposed = true;

            if (_connection != null)
            {
                await _connection.CloseAsync();
                await _connection.DisposeAsync();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing RabbitMQ connection");
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
}
