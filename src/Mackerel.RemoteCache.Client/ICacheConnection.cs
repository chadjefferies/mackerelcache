using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Client.Encoding;
using Mackerel.RemoteCache.Client.Routing;
using Mackerel.RemoteCache.Client.Watch;

namespace Mackerel.RemoteCache.Client
{
    /// <summary>
    /// Represents a connection to one or many cache nodes. Meant to be used as a singleton.
    /// </summary>
    public interface ICacheConnection : IAsyncDisposable
    {
        Action<Exception> ErrorHandler { get; set; }

        /// <summary>
        /// Iterates all partitions and returns matching results. 
        /// </summary>
        /// <remarks>
        /// Only offers limited guarantees about the returned elements since the collection 
        /// that we iterate may change during the iteration process.
        /// </remarks>
        IAsyncEnumerable<PartitionStats> ScanPartitionsAsync(string pattern, int count, CancellationToken token = default);

        /// <summary>
        /// Returns partition level stats for a single partition.
        /// </summary>
        Task<PartitionStats> GetPartitionStatsAsync(string partitionKey, CancellationToken token = default);

        /// <summary>
        /// Configures a partition in the cache. If it already exists,
        /// it's updated, if it doesn't exist, a new partition is created.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="expiration"></param>
        /// <param name="expirationType"></param>
        /// <param name="persist">Whether or not the partition metadata should be persisted.</param>
        /// <param name="evictionPolicy">The eviction policy to use for this partition. If <see cref="EvictionPolicy.NoEviction"/> is used and the cache is out of capacity, no new keys will be accepted.</param>
        /// <param name="maxCacheSize">The amount of space to reserve for this partition. If there are multiple cache nodes, this size is reserved on each of them.</param>
        /// <param name="token"></param>
        Task PutPartitionAsync(string partitionKey, TimeSpan expiration, ExpirationType expirationType, bool persist, EvictionPolicy evictionPolicy, long maxCacheSize, CancellationToken token = default);

        /// <summary>
        /// Removes an entire partition and all it's data from the cache.
        /// </summary>
        Task DeletePartitionAsync(string partitionKey, CancellationToken token = default);

        /// <summary>
        /// Clears data for an entire partition.
        /// </summary>
        Task FlushPartitionAsync(string partitionKey, CancellationToken token = default);

        /// <summary>
        /// Returns the global stats for all cache nodes such as hits, misses, etc
        /// </summary>
        Task<CacheStats> GetStatsAsync(CancellationToken token = default);

        /// <summary>
        /// Creates a strongly typed implementation used to access the key-value operations of the cache.
        /// </summary>
        ICache<T> GetCache<T>(ICacheCodec<T> codec, IHashFunction hashFunction, IRouter router);

        /// <summary>
        /// Gets the collection of cache node channels used by this connection. 
        /// Can be used to execute commands against individual cache nodes.
        /// </summary>
        IReadOnlyList<ICacheNodeChannel> GetNodes();
    }
}
