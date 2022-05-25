using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Client.Watch;

namespace Mackerel.RemoteCache.Client
{
    /// <summary>
    /// A typed cache reference used to access a remote cache.
    /// </summary>
    public interface ICache<T> : IAsyncDisposable
    {
        /// <summary>
        /// Removes the value in the cache at the specified key.
        /// </summary>
        Task DeleteAsync(string partitionKey, string key, CancellationToken token = default);
        /// <summary>
        /// Removes values in the cache at the specified keys.
        /// </summary>
        Task<int> DeleteAsync(string partitionKey, IReadOnlyCollection<string> keys, CancellationToken token = default);

        /// <summary>
        /// Gets the value in the cache at the specified key.
        /// </summary>
        Task<T> GetAsync(string partitionKey, string key, CancellationToken token = default);
        /// <summary>
        /// Gets values in the cache at the specified keys.
        /// </summary>
        Task<IDictionary<string, T>> GetAsync(string partitionKey, IReadOnlyCollection<string> keys, CancellationToken token = default);

        /// <summary>
        /// Sets a key-value in the cache. If it already exists,
        /// it's updated, if it doesn't exist, a new entry is created.
        /// </summary>
        Task PutAsync(string partitionKey, string key, T value, CancellationToken token = default);
        /// <summary>
        /// Sets a collection of key-value pairs in the cache. If they already exist,
        /// they are updated, if they don't exist, new entries are created.
        /// </summary>
        Task PutAsync(string partitionKey, IReadOnlyCollection<KeyValuePair<string, T>> items, CancellationToken token = default);

        /// <summary>
        /// Sets a key-value in the cache only if it doesn't already exist.
        /// </summary>
        Task PutIfNotExistsAsync(string partitionKey, string key, T value, CancellationToken token = default);
        /// <summary>
        /// Sets a collection of key-value pairs in the cache only if they don't already exist.
        /// </summary>
        Task PutIfNotExistsAsync(string partitionKey, IReadOnlyCollection<KeyValuePair<string, T>> items, CancellationToken token = default);

        /// <summary>
        /// Sets a key-value in the cache only if it exists already.
        /// </summary>
        Task PutIfExistsAsync(string partitionKey, string key, T value, CancellationToken token = default);
        /// <summary>
        /// Sets a collection of key-value pairs in the cache only if they exist already.
        /// </summary>
        Task PutIfExistsAsync(string partitionKey, IReadOnlyCollection<KeyValuePair<string, T>> items, CancellationToken token = default);

        /// <summary>
        /// Iterates all keys in a given partition and returns matching results. 
        /// </summary>
        /// <remarks>
        /// Only offers limited guarantees about the returned elements since the collection 
        /// that we iterate may change during the iteration process.
        /// </remarks>
        IAsyncEnumerable<(string Key, T Value, int Offset)> ScanKeysAsync(string partitionKey, string pattern, int count, int offset, CancellationToken token = default);

        /// <summary>
        /// Returns the remaining time to live of a key.
        /// </summary>
        Task<TimeSpan> TtlAsync(string partitionKey, string key, CancellationToken token = default);
        /// <summary>
        /// Returns the remaining time to live of the specified keys.
        /// </summary>
        Task<IDictionary<string, TimeSpan>> TtlAsync(string partitionKey, IReadOnlyCollection<string> keys, CancellationToken token = default);

        /// <summary>
        /// Updates the last access time of a key.
        /// </summary>
        /// <remarks>
        /// Only supported with sliding expiration.
        /// </remarks>
        Task TouchAsync(string partitionKey, string key, CancellationToken token = default);
        /// <summary>
        /// Updates the last access time of the specified keys.
        /// </summary>
        /// <remarks>
        /// Only supported with sliding expiration.
        /// </remarks>
        Task<int> TouchAsync(string partitionKey, IReadOnlyCollection<string> keys, CancellationToken token = default);

        /// <summary>
        /// Increments a number stored at a key.
        /// </summary>
        Task<long> IncrementAsync(string partitionKey, string key, CancellationToken token = default);
        /// <summary>
        /// Increments a number stored at a key by the requested value.
        /// </summary>
        Task<long> IncrementByAsync(string partitionKey, string key, long value, CancellationToken token = default);

        /// <summary>
        /// Decrements a number stored at a key.
        /// </summary>
        Task<long> DecrementAsync(string partitionKey, string key, CancellationToken token = default);
        /// <summary>
        /// Decrements a number stored at a key by the requested value.
        /// </summary>
        Task<long> DecrementByAsync(string partitionKey, string key, long value, CancellationToken token = default);

        CancellationToken WatchToken { get; }

        /// <summary>
        /// Watches for changes to keys across an entire partition.
        /// </summary>
        Task WatchAsync(string watchId, string partitionKey, Action<WatchEvent<T>> handler, CancellationToken token = default);
        /// <summary>
        /// Watches for changes to keys across an entire partition.
        /// </summary>
        Task WatchAsync(string watchId, string partitionKey, IReadOnlyList<WatchEventType> filters, Action<WatchEvent<T>> handler, CancellationToken token = default);

        /// <summary>
        /// Watches for changes to a specific key.
        /// </summary>
        Task WatchAsync(string watchId, string partitionKey, string key, Action<WatchEvent<T>> handler, CancellationToken token = default);
        /// <summary>
        /// Watches for changes to a specific key.
        /// </summary>
        Task WatchAsync(string watchId, string partitionKey, string key, IReadOnlyList<WatchEventType> filters, Action<WatchEvent<T>> handler, CancellationToken token = default);

        /// <summary>
        /// Cancels an existing watch that was created on the current stream.
        /// </summary>
        Task CancelAsync(string watchId, string partitionKey);
    }
}
