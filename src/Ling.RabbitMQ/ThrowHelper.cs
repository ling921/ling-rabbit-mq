using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Ling.RabbitMQ;

/// <summary>
/// Provides helper methods for throwing exceptions.
/// </summary>
internal static class ThrowHelper
{
    /// <summary>
    /// Throws an <see cref="ArgumentNullException"/> if the specified argument is null.
    /// </summary>
    /// <typeparam name="T">The type of the argument.</typeparam>
    /// <param name="argument">The argument to check.</param>
    /// <param name="paramName">The name of the parameter. This is automatically provided by the compiler.</param>
    /// <exception cref="ArgumentNullException">Thrown when the argument is null.</exception>
    internal static void ThrowIfNull<T>([NotNull] T? argument, [CallerArgumentExpression(nameof(argument))] string? paramName = null)
    {
        if (argument is null)
        {
            throw new ArgumentNullException(paramName);
        }
    }

    /// <summary>
    /// Throws an <see cref="ArgumentNullException"/> if the specified string argument is null,
    /// or an <see cref="ArgumentException"/> if the string argument is whitespace.
    /// </summary>
    /// <param name="argument">The string argument to check.</param>
    /// <param name="paramName">The name of the parameter. This is automatically provided by the compiler.</param>
    /// <exception cref="ArgumentNullException">Thrown when the argument is null.</exception>
    /// <exception cref="ArgumentException">Thrown when the argument is whitespace.</exception>
    internal static void IfNullOrWhitespace([NotNull] string? argument, [CallerArgumentExpression(nameof(argument))] string? paramName = null)
    {
        if (string.IsNullOrWhiteSpace(argument))
        {
            if (argument is null)
            {
                throw new ArgumentNullException(paramName);
            }
            throw new ArgumentException("Argument is whitespace", paramName);
        }
    }
}
