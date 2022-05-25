using System;

namespace Mackerel.RemoteCache.Client.Routing
{
    /// <summary>
    /// Uses the client's account name to route requests to nodes. 
    /// As a result, a single client will always talk to the same cache node.
    /// </summary>
    public class ClientAccountRouter : IRouter
    {
        public string GetRouteKey(string partitionKey, string key)
            => $"{Environment.UserDomainName}-{Environment.UserName}";
    }
}
