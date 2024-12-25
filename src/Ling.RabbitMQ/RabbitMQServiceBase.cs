using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using System.Diagnostics.CodeAnalysis;

namespace Ling.RabbitMQ;

/// <summary>
/// Provides a base class for RabbitMQ services, handling connection and channel management.
/// </summary>
public abstract class RabbitMQServiceBase : IDisposable, IAsyncDisposable
{
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private bool _disposed;
    private bool _isOwnedConnection;

    /// <summary>
    /// Gets the logger instance.
    /// </summary>
    protected ILogger Logger { get; }

    /// <summary>
    /// Gets the RabbitMQ connection configuration.
    /// </summary>
    protected RabbitMQOptions? ConnectionConfig { get; }

    /// <summary>
    /// Gets the RabbitMQ connection instance.
    /// <para>
    /// Ensure to call <see cref="InitializeAsync(CancellationToken)"/> before using this property to prevent null value.
    /// </para>
    /// </summary>
    protected IConnection? Connection { get; private set; }

    /// <summary>
    /// Gets the RabbitMQ channel instance.
    /// <para>
    /// Ensure to call <see cref="InitializeAsync(CancellationToken)"/> before using this property to prevent null value.
    /// </para>
    /// </summary>
    protected IChannel? Channel { get; private set; }

    /// <summary>
    /// Gets the message serializer instance.
    /// </summary>
    protected IMessageSerializer Serializer { get; }

    /// <summary>
    /// Gets the options for creating a channel.
    /// </summary>
    protected virtual CreateChannelOptions? ChannelOptions { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMQServiceBase"/> class with the specified connection.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection.</param>
    protected RabbitMQServiceBase(IConnection connection) : this(connection, new DefaultMessageSerializer(), NullLoggerFactory.Instance)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMQServiceBase"/> class with the specified connection configuration.
    /// </summary>
    /// <param name="connectionConfig">The RabbitMQ connection configuration.</param>
    protected RabbitMQServiceBase(RabbitMQOptions connectionConfig) : this(connectionConfig, new DefaultMessageSerializer(), NullLoggerFactory.Instance)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMQServiceBase"/> class with the specified
    /// connection, serializer, and logger factory.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection configuration.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    protected RabbitMQServiceBase(IConnection connection, IMessageSerializer serializer, ILoggerFactory loggerFactory)
    {
        Connection = connection;
        Serializer = serializer;
        Logger = loggerFactory.CreateLogger(GetType());
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMQServiceBase"/> class with the specified
    /// connection configuration, serializer, and logger factory.
    /// </summary>
    /// <param name="connectionConfig">The RabbitMQ connection configuration.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    protected RabbitMQServiceBase(RabbitMQOptions connectionConfig, IMessageSerializer serializer, ILoggerFactory loggerFactory)
    {
        ConnectionConfig = connectionConfig;
        Serializer = serializer;
        Logger = loggerFactory.CreateLogger(GetType());
    }

    /// <summary>
    /// Initializes the RabbitMQ connection and channel asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    [MemberNotNull(nameof(Connection), nameof(Channel))]
#pragma warning disable CS8774 // Member must have a non-null value when exiting.
    protected async ValueTask InitializeAsync(CancellationToken cancellationToken)
    {
        if (Connection is null)
        {
            if (ConnectionConfig is null)
            {
                Logger.LogError("'ConnectionConfig' is not null");
                throw new InvalidOperationException("'ConnectionConfig' cannot be null.");
            }

            await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

            try
            {
                // Double-check pattern
                if (Connection is null)
                {
                    var factory = new ConnectionFactory
                    {
                        HostName = ConnectionConfig.HostName,
                        Port = ConnectionConfig.Port,
                        VirtualHost = ConnectionConfig.VirtualHost,
                        UserName = ConnectionConfig.UserName,
                        Password = ConnectionConfig.Password,
                        RequestedHeartbeat = TimeSpan.FromSeconds(ConnectionConfig.RequestedHeartbeat),
                        AutomaticRecoveryEnabled = ConnectionConfig.AutomaticRecoveryEnabled,
                        NetworkRecoveryInterval = TimeSpan.FromSeconds(ConnectionConfig.NetworkRecoveryInterval),
                        ConsumerDispatchConcurrency = ConnectionConfig.ConsumerDispatchConcurrency,
                    };

                    Connection = await factory.CreateConnectionAsync(cancellationToken);
                    _isOwnedConnection = true;

                    Logger.LogInformation("RabbitMQ connection established successfully");
                }
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Failed to establish RabbitMQ connection");
                throw;
            }
            finally
            {
                _semaphore.Release();
            }
        }

        if (Channel is null)
        {
            await _semaphore.WaitAsync(cancellationToken);

            try
            {
                // Double-check pattern
                if (Channel is null)
                {
                    Channel = await Connection.CreateChannelAsync(ChannelOptions, cancellationToken);

                    Logger.LogInformation("RabbitMQ channel created successfully");
                }
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Failed to create channel");
                throw;
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }
#pragma warning restore CS8774 // Member must have a non-null value when exiting.

    #region Dispose

    /// <summary>
    /// Disposes the RabbitMQ connection and channel.
    /// </summary>
    /// <param name="disposing">A value indicating whether the method is being called from the Dispose method.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                try
                {
                    if (Channel is not null)
                    {
                        Channel.Dispose();

                        Logger.LogTrace("[sync] RabbitMQ channel disposed successfully");
                    }

                    if (_isOwnedConnection && Connection is not null)
                    {
                        Connection.Dispose();

                        Logger.LogTrace("[sync] RabbitMQ connection disposed successfully");
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, "Error disposing RabbitMQ connection or channel");
                }
            }

            _disposed = true;
        }
    }

    /// <summary>
    /// Disposes the RabbitMQ connection and channel.
    /// </summary>
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Disposes any additional resources asynchronously.
    /// </summary>
    /// <param name="disposing">A value indicating whether the method is being called from the DisposeAsync method.</param>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    protected virtual async ValueTask DisposeAsync(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                try
                {
                    if (Channel is not null)
                    {
                        await Channel.DisposeAsync();

                        Logger.LogTrace("[async] RabbitMQ channel disposed successfully");
                    }

                    if (_isOwnedConnection && Connection is not null)
                    {
                        await Connection.DisposeAsync();

                        Logger.LogTrace("[async] RabbitMQ connection disposed successfully");
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, "Error disposing RabbitMQ connection or channel");
                }
            }

            _disposed = true;
        }
    }

    /// <summary>
    /// Disposes the RabbitMQ connection and channel asynchronously.
    /// </summary>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        await DisposeAsync(true);
        GC.SuppressFinalize(this);
    }

    #endregion
}
