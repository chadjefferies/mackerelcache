namespace Mackerel.RemoteCache.Client.Routing
{
    /// <summary>
    /// Uses the cache key to route requests to nodes. 
    /// As a result, keys in a partition will be spread evenly across cache nodes.
    /// </summary>
    public class KeyRouter : IRouter
    {
        public string GetRouteKey(string _, string key)
            => key;
    }
}
