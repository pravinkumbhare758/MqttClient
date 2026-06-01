using System.Text.RegularExpressions;
using MqttClient.Interfaces;

namespace MqttClient.Services;

public sealed class TopicRouter : ITopicRouter
{
    // Plain array: FrozenDictionary<Regex,…> provided no lookup benefit because
    // Regex has only identity equality — iteration was linear anyway.
    private readonly (Regex Pattern, IMqttMessageHandler Handler)[] _routes;

    public IReadOnlyList<string> RegisteredFilters { get; }

    public TopicRouter(IEnumerable<IMqttMessageHandler> handlers)
    {
        var list = handlers
            .Select(h => (
                Pattern: new Regex(
                    MqttTopicToRegex(h.TopicFilter),
                    RegexOptions.Compiled | RegexOptions.CultureInvariant),
                Handler: h))
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

    // Converts MQTT wildcard filter to a regex:  +  →  [^/]+   #  →  .+
    private static string MqttTopicToRegex(string filter)
    {
        var escaped = Regex.Escape(filter)
            .Replace(@"\+", "[^/]+")
            .Replace(@"\#", ".+");
        return $"^{escaped}$";
    }

    // Best-effort overlap detection: generate a representative topic for each
    // filter and test it against every other filter's compiled regex.
    // Catches the common problematic patterns (e.g. "devices/#" vs "devices/+/telemetry").
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
