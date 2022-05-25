namespace Mackerel.RemoteCache.Server.Runtime
{
    /// <summary>
    /// Turn away now, a mutable struct!
    /// </summary>
    public struct CacheEntry
    {
        public CacheKey Key { get; set; }
        public CacheValue Value { get; set; }
    }
}
