namespace Mackerel.RemoteCache.Client.Routing
{
    /// <summary>
    /// Defines how routing keys get mapped to nodes.
    /// </summary>
    public interface IHashFunction
    {
        ICacheNodeChannel Hash(string routeKey);
    }
}
