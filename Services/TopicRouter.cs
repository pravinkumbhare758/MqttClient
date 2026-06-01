using System.Text.RegularExpressions;
using MqttClient.Interfaces;

namespace MqttClient.Services;

public sealed class TopicRouter : ITopicRouter
{
    private readonly (Regex Pattern, IMqttMessageHandler Handler)[] _routes;

    public IReadOnlyList<string> RegisteredFilters { get; }

    public TopicRouter(IEnumerable<IMqttMessageHandler> handlers)
    {
        var list = handlers
            .Select(h =>
            {
                ValidateFilter(h.TopicFilter);
                return (
                    Pattern: new Regex(
                        MqttTopicToRegex(h.TopicFilter),
                        RegexOptions.Compiled | RegexOptions.CultureInvariant),
                    Handler: h);
            })
            .ToArray();

        CheckForOverlaps(list);

        _routes = list;
        RegisteredFilters = list.Select(r => r.Handler.TopicFilter).ToList().AsReadOnly();
    }

    public IMqttMessageHandler? Resolve(string topic)
    {
        foreach (var (pattern, handler) in _routes)
            if (pattern.IsMatch(topic)) return handler;
        return null;
    }

    // MQTT spec §4.7: validate filter structure before converting to regex.
    // Catches malformed filters like "a/#/b" or "a+b" that would produce
    // incorrect matches without throwing at subscribe time.
    private static void ValidateFilter(string filter)
    {
        if (string.IsNullOrEmpty(filter))
            throw new ArgumentException("Topic filter must not be empty.");

        var segments = filter.Split('/');

        for (int i = 0; i < segments.Length; i++)
        {
            var seg = segments[i];

            // '+' must be the only character in its segment
            if (seg.Contains('+') && seg != "+")
                throw new ArgumentException(
                    $"Topic filter '{filter}' is invalid: '+' must occupy its entire segment.");

            // '#' must be the only character in its segment AND must be the last segment
            if (seg.Contains('#'))
            {
                if (seg != "#")
                    throw new ArgumentException(
                        $"Topic filter '{filter}' is invalid: '#' must occupy its entire segment.");
                if (i != segments.Length - 1)
                    throw new ArgumentException(
                        $"Topic filter '{filter}' is invalid: '#' must be the last segment.");
            }
        }
    }

    // Converts MQTT wildcard filter to regex:  +  →  [^/]+   #  →  .+
    private static string MqttTopicToRegex(string filter)
    {
        var escaped = Regex.Escape(filter)
            .Replace(@"\+", "[^/]+")
            .Replace(@"\#", ".+");
        return $"^{escaped}$";
    }

    // Best-effort overlap detection: generates a representative topic for each filter
    // and tests it against every other filter's compiled regex.
    // Catches common conflicts (e.g. "devices/#" vs "devices/+/telemetry").
    // Note: does not catch all possible conflicts (e.g. "+/a" vs "b/+") — document
    // this limitation in handler registrations.
    private static void CheckForOverlaps(
        ReadOnlySpan<(Regex Pattern, IMqttMessageHandler Handler)> routes)
    {
        for (int i = 0; i < routes.Length; i++)
        {
            var sample = ToRepresentativeTopic(routes[i].Handler.TopicFilter);
            for (int j = 0; j < routes.Length; j++)
            {
                if (i == j) continue;
                if (routes[j].Pattern.IsMatch(sample))
                    throw new InvalidOperationException(
                        $"Topic filter '{routes[i].Handler.TopicFilter}' overlaps with " +
                        $"'{routes[j].Handler.TopicFilter}'. Each topic must be owned by " +
                        $"exactly one handler.");
            }
        }
    }

    private static string ToRepresentativeTopic(string filter) =>
        filter.Replace("+", "x").Replace("#", "x/y");
}
