using System.Collections.Frozen;
using System.Text.RegularExpressions;
using MqttClient.Interfaces;

namespace MqttClient.Services;

public sealed class TopicRouter : ITopicRouter
{
    // Maps compiled regex -> handler (built once at startup)
    private readonly FrozenDictionary<Regex, IMqttMessageHandler> _routes;

    public IReadOnlyList<string> RegisteredFilters { get; }

    public TopicRouter(IEnumerable<IMqttMessageHandler> handlers)
    {
        var routes = new Dictionary<Regex, IMqttMessageHandler>();
        var filters = new List<string>();

        foreach (var handler in handlers)
        {
            var pattern = MqttTopicToRegex(handler.TopicFilter);
            routes[new Regex(pattern, RegexOptions.Compiled | RegexOptions.CultureInvariant)] = handler;
            filters.Add(handler.TopicFilter);
        }

        _routes = routes.ToFrozenDictionary();
        RegisteredFilters = filters.AsReadOnly();
    }

    public IMqttMessageHandler? Resolve(string topic)
    {
        foreach (var (regex, handler) in _routes)
        {
            if (regex.IsMatch(topic))
                return handler;
        }
        return null;
    }

    // Converts MQTT wildcard filter to regex: + → [^/]+, # → .+
    private static string MqttTopicToRegex(string filter)
    {
        var escaped = Regex.Escape(filter)
            .Replace(@"\+", "[^/]+")
            .Replace(@"\#", ".+");
        return $"^{escaped}$";
    }
}
