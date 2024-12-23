using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Ling.RabbitMQ;

/// <summary>
/// Defines methods for serializing and deserializing messages.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// Serializes a message to a byte array.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="message">The message to serialize.</param>
    /// <returns>The serialized message as a byte array.</returns>
    byte[] Serialize<T>(T message);

    /// <summary>
    /// Deserializes a message from a byte span.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="messageBytes">The byte span containing the message.</param>
    /// <returns>The deserialized message.</returns>
    [return: MaybeNull]
    T? Deserialize<T>(ReadOnlySpan<byte> messageBytes);
}

/// <summary>
/// Default implementation of <see cref="IMessageSerializer"/> that uses System.Text.Json for serialization.
/// </summary>
internal sealed class DefaultMessageSerializer : IMessageSerializer
{
#if NET9_0_OR_GREATER
    private static readonly JsonSerializerOptions _jsonSerializerOptions = JsonSerializerOptions.Web;
#else                                    
    private static readonly JsonSerializerOptions _jsonSerializerOptions = new(JsonSerializerDefaults.Web);
#endif

    /// <inheritdoc/>
    public byte[] Serialize<T>(T message)
    {
        return JsonSerializer.SerializeToUtf8Bytes(message, _jsonSerializerOptions);
    }

    /// <inheritdoc/>
    [return: MaybeNull]
    public T? Deserialize<T>(ReadOnlySpan<byte> messageBytes)
    {
        return JsonSerializer.Deserialize<T>(messageBytes, _jsonSerializerOptions);
    }
}
