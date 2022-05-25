namespace Mackerel.RemoteCache.Client.Routing
{
    /// <summary>
    /// Uses the partition key to route requests to nodes. 
    /// As a result, all keys in a partition will reside on the same cache node.
    /// </summary>
    public class PartitionRouter : IRouter
    {
        public string GetRouteKey(string partitionKey, string _)
            => partitionKey;
    }
}
