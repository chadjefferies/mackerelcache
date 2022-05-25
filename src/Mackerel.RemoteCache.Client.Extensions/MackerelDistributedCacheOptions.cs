using System;
using Mackerel.RemoteCache.Api.V1;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace Mackerel.RemoteCache.Client.Extensions
{
    /// <summary>
    /// Configuration options for <see cref="MackerelDistributedCache"/>.
    /// </summary>
    public class MackerelDistributedCacheOptions : IOptions<MackerelDistributedCacheOptions>
    {
        /// <summary>
        /// The cache partition used to store data for this application.
        /// </summary>
        public string Partition { get; set; }

        /// <summary>
        /// Whether or not to keep a local, synchronized version of cached data.
        /// </summary>
        /// <remarks>
        /// Keeps a near (in-process) cache that is synchronized with the remote cache. The near cache is fast, but limited in size. 
        /// It is up to the developer to limit the cache size using the settings provided by <see cref="IMemoryCache"/>.
        /// If your read rate is much higher than write rate and eventual consistency can be tolerated, then a near-cache makes sense.
        /// </remarks>
        public bool UseNearCache { get; set; }

        /// <summary>
        /// The policy to use when the cache is out of capacity.
        /// </summary>
        public EvictionPolicy EvictionPolicy { get; set; }

        /// <summary>
        /// Sets the expiration type on the partition. If this is not set, the expiration type for the partition will be taken from the <see cref="DistributedCacheEntryOptions" /> of the first item.
        /// </summary>
        /// <remarks>
        /// Configuring expiration values is only supported at the partition level and not at the individual item level.
        /// Setting the values here will override any configuration done at the item level via the <see cref="DistributedCacheEntryOptions" />.
        /// </remarks>
        public ExpirationType? ExpirationType { get; set; }

        /// <summary>
        /// Configures how long keys in this partition should live for.
        /// </summary>
        /// <remarks>
        /// Configuring expiration values is only supported at the partition level and not at the individual item level.
        /// If this is not set, the expiration value for the partition will be taken from the <see cref="DistributedCacheEntryOptions" /> of the first item.
        /// Setting the values here will override any configuration done at the <see cref="DistributedCacheEntryOptions" /> item level.
        /// </remarks>
        public TimeSpan? Expiration { get; set; }

        /// <summary>
        /// The amount of space to reserve, in bytes, in the cache for this application.
        /// </summary>
        /// <remarks>
        /// If there are multiple cache nodes, this size is reserved on each of them. Defaults to zero, or unbounded.
        /// </remarks>
        public long CacheSize { get; set; }

        public MackerelDistributedCacheOptions Value => this;
    }
}
