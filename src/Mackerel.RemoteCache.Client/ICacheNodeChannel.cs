using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Mackerel.RemoteCache.Api.V1;

namespace Mackerel.RemoteCache.Client
{
    /// <summary>
    /// Represents a long-lived connection to a single cache node.
    /// </summary>
    public interface ICacheNodeChannel : IAsyncDisposable
    {
        string Address { get; }

        // basic key-value operations
        Task<NodeOperationResult<WriteResult>> DeleteAsync(string partitionKey, string key, CancellationToken token = default);
        Task<NodeOperationResult<int>> DeleteAsync(string partitionKey, IEnumerable<string> keys, CancellationToken token = default);

        Task<NodeOperationResult<ByteString>> GetAsync(string partitionKey, string key, CancellationToken token = default);
        Task<NodeOperationResult<IDictionary<string, ByteString>>> GetAsync(string partitionKey, IEnumerable<string> keys, CancellationToken token = default);

        Task<NodeOperationResult<WriteResult>> PutAsync(string partitionKey, string key, ByteString value, CancellationToken token = default);
        Task<NodeOperationResult<WriteResult>> PutAsync(string partitionKey, IEnumerable<KeyValuePair<string, ByteString>> items, CancellationToken token = default);

        Task<NodeOperationResult<WriteResult>> PutIfNotExistsAsync(string partitionKey, string key, ByteString value, CancellationToken token = default);
        Task<NodeOperationResult<WriteResult>> PutIfNotExistsAsync(string partitionKey, IEnumerable<KeyValuePair<string, ByteString>> items, CancellationToken token = default);

        Task<NodeOperationResult<WriteResult>> PutIfExistsAsync(string partitionKey, string key, ByteString value, CancellationToken token = default);
        Task<NodeOperationResult<WriteResult>> PutIfExistsAsync(string partitionKey, IEnumerable<KeyValuePair<string, ByteString>> items, CancellationToken token = default);

        Task<NodeOperationResult<WriteResult>> TouchAsync(string partitionKey, string key, CancellationToken token = default);
        Task<NodeOperationResult<int>> TouchAsync(string partitionKey, IEnumerable<string> keys, CancellationToken token = default);

        Task<NodeOperationResult<long>> TtlAsync(string partitionKey, string key, CancellationToken token = default);
        Task<NodeOperationResult<IReadOnlyList<KeyValuePair<string, long>>>> TtlAsync(string partitionKey, IEnumerable<string> keys, CancellationToken token = default);

        Task<NodeOperationResult<long>> IncrementAsync(string partitionKey, string key, CancellationToken token = default);
        Task<NodeOperationResult<long>> IncrementByAsync(string partitionKey, string key, long value, CancellationToken token = default);

        Task<NodeOperationResult<long>> DecrementAsync(string partitionKey, string key, CancellationToken token = default);
        Task<NodeOperationResult<long>> DecrementByAsync(string partitionKey, string key, long value, CancellationToken token = default);

        // utility operations
        Task<NodeOperationResult<WriteResult>> DeletePartitionAsync(string partitionKey, CancellationToken token = default);
        Task<NodeOperationResult> FlushPartitionAsync(string partitionKey, CancellationToken token = default);

        Task<NodeOperationResult<PartitionStats>> GetPartitionStatsAsync(string partitionKey, CancellationToken token = default);
        Task<NodeOperationResult<WriteResult>> PutPartitionAsync(string partitionKey, TimeSpan expiration, ExpirationType expirationType, bool persist, EvictionPolicy evictionPolicy, long maxCacheSize, CancellationToken token = default);

        AsyncServerStreamingCall<ScanPartitionsResponse> ScanPartitionsAsync(string pattern, int count, CancellationToken token = default);
        AsyncServerStreamingCall<ScanKeysResponse> ScanKeysAsync(string partitionKey, string pattern, int count, int offset, CancellationToken token = default);

        /// <summary>
        /// Removes all data from the cache node.
        /// </summary>
        Task<NodeOperationResult> FlushAllAsync(CancellationToken token = default);
        /// <summary>
        /// Force a GC run on the cache node. This is not recommended.
        /// </summary>
        Task<NodeOperationResult> InvokeGCAsync(CancellationToken token = default);

        /// <summary>
        /// Returns the global stats for the cache node such as hits, misses, etc
        /// </summary>
        Task<NodeOperationResult<CacheStats>> GetStatsAsync(CancellationToken token = default);
        /// <summary>
        /// Returns the configuration settings for the cache node.
        /// </summary>
        Task<NodeOperationResult<CacheConfiguration>> GetConfAsync(CancellationToken token = default);
        /// <summary>
        /// Used to determine if the cache node is available.
        /// </summary>
        Task<NodeOperationResult<string>> PingAsync(CancellationToken token = default);

        // watch operations
        AsyncDuplexStreamingCall<WatchRequest, WatchResponse> WatchAsync(CancellationToken token = default);
    }
}
