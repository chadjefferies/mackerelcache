namespace Mackerel.RemoteCache.Client.Routing
{
    /// <summary>
    /// Defines how cache entries should be routed to nodes.
    /// </summary>
    public interface IRouter
    {
        string GetRouteKey(string partitionKey, string key);
    }
}
