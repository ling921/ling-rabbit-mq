using System.Text.RegularExpressions;

namespace Ling.RabbitMQ;

/// <summary>
/// Provides an interface for validating topic patterns according to RabbitMQ topic rules.
/// </summary>
public interface ITopicPatternVerifier
{
    /// <summary>
    /// Validates if the topic pattern is valid according to RabbitMQ topic rules.
    /// </summary>
    /// <param name="topicPattern">The topic pattern to validate.</param>
    /// <param name="errorMessage">When this method returns <see langword="false"/>, contains the reason why the pattern is invalid.</param>
    /// <returns><see langword="true"/> if the pattern is valid; otherwise, <see langword="false"/>.</returns>
    /// <remarks>
    /// Valid topic patterns must follow these rules:
    /// <code>
    /// - Words are delimited by dots
    /// - * (star) can substitute for exactly one word
    /// - # (hash) can substitute for zero or more words
    /// - Maximum length is 255 bytes
    /// </code>
    /// Examples:
    /// <code>
    /// - "sport.football.premier_league"
    /// - "sport.*"
    /// - "sport.#"
    /// - "sport.*.#"
    /// </code>
    /// </remarks>
    bool IsValid(string topicPattern, out string errorMessage);
}

/// <summary>
/// Default implementation of <see cref="ITopicPatternVerifier"/> that validates topic patterns according to RabbitMQ topic rules.
/// </summary>
public sealed partial class DefaultTopicPatternVerifier : ITopicPatternVerifier
{
#if NET7_0_OR_GREATER
    [GeneratedRegex("^[a-zA-Z0-9_.*#]+$")]
    private static partial Regex TopicPatternRegex();
#else
    private static Regex TopicPatternRegex() => new("^[a-zA-Z0-9_.*#]+$", RegexOptions.Compiled);
#endif

    /// <inheritdoc />
    public bool IsValid(string topicPattern, out string errorMessage)
    {
        if (string.IsNullOrWhiteSpace(topicPattern))
        {
            errorMessage = "Topic pattern cannot be null or empty.";
            return false;
        }

        if (System.Text.Encoding.UTF8.GetByteCount(topicPattern) > 255)
        {
            errorMessage = "Topic pattern length cannot exceed 255 bytes.";
            return false;
        }

        if (!TopicPatternRegex().IsMatch(topicPattern))
        {
            errorMessage = "Topic pattern can only contain letters, numbers, dots, underscores, asterisks, and hash symbols.";
            return false;
        }

        var segments = topicPattern.Split('.');

        if (segments.Any(string.IsNullOrEmpty))
        {
            errorMessage = "Topic pattern cannot contain empty segments.";
            return false;
        }

        foreach (var segment in segments)
        {
            if (segment.Contains('*') && segment.Contains('#'))
            {
                errorMessage = "A topic segment cannot contain both * and # symbols.";
                return false;
            }

            if ((segment.Contains('*') || segment.Contains('#')) && segment.Length > 1)
            {
                errorMessage = "The * and # symbols must be used as standalone words.";
                return false;
            }
        }

        errorMessage = string.Empty;
        return true;
    }
}
