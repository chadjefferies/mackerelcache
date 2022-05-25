using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Client.Configuration;
using Mackerel.RemoteCache.Client.Internal;
using Mackerel.RemoteCache.Client.Util;

namespace Mackerel.RemoteCache.Client
{
    /// <summary>
    /// Represents a connection to one or many cache nodes. Meant to be used as a singleton.
    /// </summary>
    public partial class CacheConnection : ICacheConnection
    {
        private readonly List<ICacheNodeChannel> _channels;

        public CacheClientOptions Options { get; }
        public Action<Exception> ErrorHandler { get; set; }

        static CacheConnection()
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        }

        /// <summary>
        /// Ctor exposed for testing or advanced use cases. Please favor <see cref="Create(string)"/>
        /// </summary>
        public CacheConnection(CacheClientOptions opt, IEnumerable<ICacheNodeChannel> channels)
        {
            Options = opt;
            _channels = channels.ToList();
        }

        public async Task DeletePartitionAsync(string partitionKey, CancellationToken token = default)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));

            var nodeCalls = new Task<NodeOperationResult<WriteResult>>[_channels.Count];

            for (int i = 0; i < _channels.Count; i++)
            {
                nodeCalls[i] = _channels[i].DeletePartitionAsync(partitionKey, token);
            }

            var results = await Task.WhenAll(nodeCalls).ConfigureAwait(false);

            for (int i = 0; i < results.Length; i++)
            {
                var result = results[i];
                if (result.Exception != null)
                {
                    throw new AggregateException(results.Select(x => x.Exception));
                }
            }
        }

        public async Task FlushPartitionAsync(string partitionKey, CancellationToken token = default)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));

            var nodeCalls = new Task<NodeOperationResult>[_channels.Count];

            for (int i = 0; i < _channels.Count; i++)
            {
                nodeCalls[i] = _channels[i].FlushPartitionAsync(partitionKey, token);
            }

            var results = await Task.WhenAll(nodeCalls).ConfigureAwait(false);

            for (int i = 0; i < results.Length; i++)
            {
                var result = results[i];
                if (result.Exception != null)
                {
                    throw new AggregateException(results.Select(x => x.Exception));
                }
            }
        }

        public async IAsyncEnumerable<PartitionStats> ScanPartitionsAsync(string pattern, int count, [EnumeratorCancellation] CancellationToken token = default)
        {
            if (pattern == null) throw new ArgumentNullException(nameof(pattern));
            if (count == 0) throw new ArgumentException("count cannot be zero", nameof(count));

            var results = new List<PartitionStats>();

            var calls = _channels.Select(x => x.ScanPartitionsAsync(pattern, count, token));
            try
            {
                var scans = new AsyncStreamReaderCombiner<ScanPartitionsResponse>(calls.Select(x => x.ResponseStream).ToList());

                await foreach (var item in scans)
                {
                    results.Add(item.Stats);
                }

                foreach (var partitionGroup in results.GroupBy(x => x.PartitionKey))
                {
                    var partitionStats = StatsFunctions.SumPartitionStats(partitionGroup);
                    yield return partitionStats;
                }
            }
            finally
            {
                foreach (var call in calls)
                {
                    call.Dispose();
                }
            }
        }

        public async Task<PartitionStats> GetPartitionStatsAsync(string partitionKey, CancellationToken token = default)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));

            var nodeCalls = new Task<NodeOperationResult<PartitionStats>>[_channels.Count];

            for (int i = 0; i < _channels.Count; i++)
            {
                nodeCalls[i] = _channels[i].GetPartitionStatsAsync(partitionKey, token);
            }

            var nodeResults = await Task.WhenAll(nodeCalls).ConfigureAwait(false);

            return StatsFunctions.SumPartitionStats(nodeResults.Select(x => x.Result));
        }

        public async Task PutPartitionAsync(string partitionKey, TimeSpan expiration, ExpirationType expirationType, bool persist, EvictionPolicy evictionPolicy, long maxCacheSize, CancellationToken token = default)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));

            var nodeCalls = new Task<NodeOperationResult<WriteResult>>[_channels.Count];

            for (int i = 0; i < _channels.Count; i++)
            {
                nodeCalls[i] = _channels[i].PutPartitionAsync(partitionKey, expiration, expirationType, persist, evictionPolicy, maxCacheSize, token);
            }

            var results = await Task.WhenAll(nodeCalls).ConfigureAwait(false);

            for (int i = 0; i < results.Length; i++)
            {
                var result = results[i];
                if (result.Exception != null)
                {
                    throw new AggregateException(results.Select(x => x.Exception));
                }
            }
        }

        public async Task<CacheStats> GetStatsAsync(CancellationToken token = default)
        {
            var nodeCalls = new Task<NodeOperationResult<CacheStats>>[_channels.Count];

            for (int i = 0; i < _channels.Count; i++)
            {
                nodeCalls[i] = _channels[i].GetStatsAsync(token);
            }

            var nodeResults = await Task.WhenAll(nodeCalls).ConfigureAwait(false);

            return StatsFunctions.SumStats(nodeResults.Select(x => x.Result));
        }

        public IReadOnlyList<ICacheNodeChannel> GetNodes() => _channels;

        public async ValueTask DisposeAsync()
        {
            foreach (var item in _channels)
            {
                await item.DisposeAsync();
            }
        }
    }
}