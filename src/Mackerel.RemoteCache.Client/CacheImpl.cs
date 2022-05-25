using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Client.Configuration;
using Mackerel.RemoteCache.Client.Encoding;
using Mackerel.RemoteCache.Client.Internal;
using Mackerel.RemoteCache.Client.Routing;
using Mackerel.RemoteCache.Client.Util;

namespace Mackerel.RemoteCache.Client
{
    /// <summary>
    /// A typed cache reference used to access a remote cache.
    /// </summary>
    public partial class CacheImpl<T> : ICache<T>
    {
        private readonly ICacheConnection _connection;
        private readonly IHashFunction _hashFunction;
        private readonly IRouter _router;
        private readonly ICacheCodec<T> _codec;

        public CacheClientOptions Options { get; }

        public CacheImpl(
            ICacheConnection connection,
            IHashFunction hashFunction,
            IRouter router,
            ICacheCodec<T> codec,
            CacheClientOptions opt)
        {
            _connection = connection;
            _hashFunction = hashFunction;
            _router = router;
            _codec = codec;
            Options = opt;
        }

        public virtual async Task DeleteAsync(string partitionKey, string key, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(key, nameof(key));

            var routeKey = _router.GetRouteKey(partitionKey, key);
            var node = _hashFunction.Hash(routeKey);
            var result = await node.DeleteAsync(
                partitionKey,
                key,
                token).ConfigureAwait(false);
            if (result.Exception != null) throw result.Exception;
        }

        public virtual async Task<int> DeleteAsync(string partitionKey, IReadOnlyCollection<string> keys, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(keys, nameof(keys));
            if (keys.Count == 0) await Task.FromResult(0);

            var opsByChannel = RouteToChannel(partitionKey, keys);
            var nodeCalls = new Task<NodeOperationResult<int>>[opsByChannel.Count];
            var j = 0;
            foreach (var pair in opsByChannel)
            {
                nodeCalls[j++] = pair.Key.DeleteAsync(partitionKey, pair.Value, token);
            }

            var nodeResponses = await Task.WhenAll(nodeCalls).ConfigureAwait(false);
            int result = 0;
            for (int i = 0; i < nodeResponses.Length; i++)
            {
                var nodeResult = nodeResponses[i];
                if (nodeResult.Exception != null)
                {
                    throw new AggregateException(nodeResponses.Select(x => x.Exception));
                }

                result += nodeResult.Result;
            }

            return result;
        }

        public virtual async Task<T> GetAsync(string partitionKey, string key, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(key, nameof(key));

            var routeKey = _router.GetRouteKey(partitionKey, key);
            var node = _hashFunction.Hash(routeKey);
            var result = await node.GetAsync(
                partitionKey,
                key,
                token).ConfigureAwait(false);
            if (result.Exception != null) throw result.Exception;
            var valDecoded = _codec.Decode(result.Result.Span, result.Result.IsEmpty);
            return valDecoded;
        }

        public virtual async Task<IDictionary<string, T>> GetAsync(string partitionKey, IReadOnlyCollection<string> keys, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(keys, nameof(keys));
            var results = new Dictionary<string, T>(keys.Count);
            if (keys.Count == 0) return results;

            var opsByChannel = RouteToChannel(partitionKey, keys);
            var nodeCalls = new Task<NodeOperationResult<IDictionary<string, ByteString>>>[opsByChannel.Count];
            var j = 0;
            foreach (var pair in opsByChannel)
            {
                nodeCalls[j++] = pair.Key.GetAsync(partitionKey, pair.Value, token);
            }

            var nodeResponses = await Task.WhenAll(nodeCalls).ConfigureAwait(false);

            for (int i = 0; i < nodeResponses.Length; i++)
            {
                var nodeResult = nodeResponses[i];
                if (nodeResult.Exception != null)
                {
                    throw new AggregateException(nodeResponses.Select(x => x.Exception));
                }

                if (nodeResult.Result == default) continue;

                foreach (var entry in nodeResult.Result)
                {
                    if (entry.Key != null)
                    {
                        var valDecoded = _codec.Decode(entry.Value.Span, entry.Value.IsEmpty);
                        results.Add(entry.Key, valDecoded);
                    }
                }
            }

            return results;
        }

        public virtual async Task PutAsync(string partitionKey, string key, T value, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(key, nameof(key));
            CachePreconditions.CheckNotNull(value, nameof(value));

            var routeKey = _router.GetRouteKey(partitionKey, key);
            var node = _hashFunction.Hash(routeKey);
            var result = await node.PutAsync(
                partitionKey,
                key,
                ByteString.CopyFrom(_codec.Encode(value)),
                token).ConfigureAwait(false);
            if (result.Exception != null) throw result.Exception;
        }

        public virtual async Task PutAsync(string partitionKey, IReadOnlyCollection<KeyValuePair<string, T>> items, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(items, nameof(items));
            if (items.Count == 0) return;

            var opsByChannel = RouteToChannel(partitionKey, items);
            var nodeCalls = new Task<NodeOperationResult<WriteResult>>[opsByChannel.Count];
            var j = 0;
            foreach (var pair in opsByChannel)
            {
                nodeCalls[j++] = pair.Key.PutAsync(partitionKey, pair.Value, token);
            }

            var nodeResponses = await Task.WhenAll(nodeCalls).ConfigureAwait(false);

            for (int i = 0; i < nodeResponses.Length; i++)
            {
                var nodeResult = nodeResponses[i];
                if (nodeResult.Exception != null)
                {
                    throw new AggregateException(nodeResponses.Select(x => x.Exception));
                }
            }
        }

        public virtual async Task PutIfExistsAsync(string partitionKey, string key, T value, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(key, nameof(key));
            CachePreconditions.CheckNotNull(value, nameof(value));

            var routeKey = _router.GetRouteKey(partitionKey, key);
            var node = _hashFunction.Hash(routeKey);
            var result = await node.PutIfExistsAsync(
                partitionKey,
                 key,
                 ByteString.CopyFrom(_codec.Encode(value)),
                token).ConfigureAwait(false);
            if (result.Exception != null) throw result.Exception;
        }

        public virtual async Task PutIfExistsAsync(string partitionKey, IReadOnlyCollection<KeyValuePair<string, T>> items, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(items, nameof(items));
            if (items.Count == 0) return;

            var opsByChannel = RouteToChannel(partitionKey, items);
            var nodeCalls = new Task<NodeOperationResult<WriteResult>>[opsByChannel.Count];
            var j = 0;
            foreach (var pair in opsByChannel)
            {
                nodeCalls[j++] = pair.Key.PutIfExistsAsync(partitionKey, pair.Value, token);
            }

            var nodeResponses = await Task.WhenAll(nodeCalls).ConfigureAwait(false);

            for (int i = 0; i < nodeResponses.Length; i++)
            {
                var nodeResult = nodeResponses[i];
                if (nodeResult.Exception != null)
                {
                    throw new AggregateException(nodeResponses.Select(x => x.Exception));
                }
            }
        }

        public virtual async Task PutIfNotExistsAsync(string partitionKey, string key, T value, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(key, nameof(key));
            CachePreconditions.CheckNotNull(value, nameof(value));

            var routeKey = _router.GetRouteKey(partitionKey, key);
            var node = _hashFunction.Hash(routeKey);
            var result = await node.PutIfNotExistsAsync(
                partitionKey,
                 key,
                 ByteString.CopyFrom(_codec.Encode(value)),
                token).ConfigureAwait(false);
            if (result.Exception != null) throw result.Exception;
        }

        public virtual async Task PutIfNotExistsAsync(string partitionKey, IReadOnlyCollection<KeyValuePair<string, T>> items, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(items, nameof(items));
            if (items.Count == 0) return;

            var opsByChannel = RouteToChannel(partitionKey, items);
            var nodeCalls = new Task<NodeOperationResult<WriteResult>>[opsByChannel.Count];
            var j = 0;
            foreach (var pair in opsByChannel)
            {
                nodeCalls[j++] = pair.Key.PutIfNotExistsAsync(partitionKey, pair.Value, token);
            }

            var nodeResponses = await Task.WhenAll(nodeCalls).ConfigureAwait(false);

            for (int i = 0; i < nodeResponses.Length; i++)
            {
                var nodeResult = nodeResponses[i];
                if (nodeResult.Exception != null)
                {
                    throw new AggregateException(nodeResponses.Select(x => x.Exception));
                }
            }
        }

        public async IAsyncEnumerable<(string Key, T Value, int Offset)> ScanKeysAsync(string partitionKey, string pattern, int count, int offset, [EnumeratorCancellation] CancellationToken token = default)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(pattern, nameof(pattern));
            if (count == 0) throw new ArgumentException("count cannot be zero", nameof(count));

            var calls = _connection.GetNodes().Select(x => x.ScanKeysAsync(partitionKey, pattern, count, offset, token));

            try
            {
                var scans = new AsyncStreamReaderCombiner<ScanKeysResponse>(calls.Select(x => x.ResponseStream).ToList());

                await foreach (var item in scans)
                {
                    if (item.Key != null)
                    {
                        var valDecoded = _codec.Decode(item.Value.Span, item.Value.IsEmpty);
                        yield return (item.Key, valDecoded, item.Index);
                    }
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

        public virtual async Task<TimeSpan> TtlAsync(string partitionKey, string key, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(key, nameof(key));

            var routeKey = _router.GetRouteKey(partitionKey, key);
            var node = _hashFunction.Hash(routeKey);
            var result = await node.TtlAsync(
                partitionKey,
                key,
                token).ConfigureAwait(false);
            if (result.Exception != null) throw result.Exception;
            var val = TimeSpan.FromMilliseconds(result.Result);
            return val;

        }

        public virtual async Task<IDictionary<string, TimeSpan>> TtlAsync(string partitionKey, IReadOnlyCollection<string> keys, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(keys, nameof(keys));
            var results = new Dictionary<string, TimeSpan>();
            if (keys.Count == 0) return results;

            var opsByChannel = RouteToChannel(partitionKey, keys);
            var nodeCalls = new Task<NodeOperationResult<IReadOnlyList<KeyValuePair<string, long>>>>[opsByChannel.Count];
            var j = 0;
            foreach (var pair in opsByChannel)
            {
                nodeCalls[j++] = pair.Key.TtlAsync(partitionKey, pair.Value, token);
            }

            var nodeResponses = await Task.WhenAll(nodeCalls).ConfigureAwait(false);

            for (int i = 0; i < nodeResponses.Length; i++)
            {
                var nodeResult = nodeResponses[i];
                if (nodeResult.Exception != null)
                {
                    throw new AggregateException(nodeResponses.Select(x => x.Exception));
                }

                if (nodeResult.Result == default) continue;

                foreach (var entry in nodeResult.Result)
                {
                    if (entry.Key != null)
                    {
                        var val = TimeSpan.FromMilliseconds(entry.Value);
                        results.Add(entry.Key, val);
                    }
                }
            }

            return results;
        }

        public virtual async Task TouchAsync(string partitionKey, string key, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(key, nameof(key));

            var routeKey = _router.GetRouteKey(partitionKey, key);
            var node = _hashFunction.Hash(routeKey);
            var result = await node.TouchAsync(
                partitionKey,
                key,
                token).ConfigureAwait(false);
            if (result.Exception != null) throw result.Exception;
        }

        public virtual async Task<int> TouchAsync(string partitionKey, IReadOnlyCollection<string> keys, CancellationToken token)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(keys, nameof(keys));
            if (keys.Count == 0) await Task.FromResult(0);

            var opsByChannel = RouteToChannel(partitionKey, keys);
            var nodeCalls = new Task<NodeOperationResult<int>>[opsByChannel.Count];
            var j = 0;
            foreach (var pair in opsByChannel)
            {
                nodeCalls[j++] = pair.Key.TouchAsync(partitionKey, pair.Value, token);
            }

            var nodeResponses = await Task.WhenAll(nodeCalls).ConfigureAwait(false);
            int result = 0;
            for (int i = 0; i < nodeResponses.Length; i++)
            {
                var nodeResult = nodeResponses[i];
                if (nodeResult.Exception != null)
                {
                    throw new AggregateException(nodeResponses.Select(x => x.Exception));
                }

                result += nodeResult.Result;
            }

            return result;
        }

        public async Task<long> IncrementAsync(string partitionKey, string key, CancellationToken token = default)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(key, nameof(key));

            var routeKey = _router.GetRouteKey(partitionKey, key);
            var node = _hashFunction.Hash(routeKey);
            var result = await node.IncrementAsync(
                partitionKey,
                key,
                token).ConfigureAwait(false);
            if (result.Exception != null) throw result.Exception;
            return result.Result;
        }

        public async Task<long> IncrementByAsync(string partitionKey, string key, long value, CancellationToken token = default)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(key, nameof(key));

            var routeKey = _router.GetRouteKey(partitionKey, key);
            var node = _hashFunction.Hash(routeKey);
            var result = await node.IncrementByAsync(
                partitionKey,
                key,
                value,
                token).ConfigureAwait(false);
            if (result.Exception != null) throw result.Exception;
            return result.Result;
        }

        public async Task<long> DecrementAsync(string partitionKey, string key, CancellationToken token = default)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(key, nameof(key));

            var routeKey = _router.GetRouteKey(partitionKey, key);
            var node = _hashFunction.Hash(routeKey);
            var result = await node.DecrementAsync(
                partitionKey,
                key,
                token).ConfigureAwait(false);
            if (result.Exception != null) throw result.Exception;
            return result.Result;
        }

        public async Task<long> DecrementByAsync(string partitionKey, string key, long value, CancellationToken token = default)
        {
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));
            CachePreconditions.CheckNotNull(key, nameof(key));

            var routeKey = _router.GetRouteKey(partitionKey, key);
            var node = _hashFunction.Hash(routeKey);
            var result = await node.DecrementByAsync(
                partitionKey,
                key,
                value,
                token).ConfigureAwait(false);
            if (result.Exception != null) throw result.Exception;
            return result.Result;
        }


        private Dictionary<ICacheNodeChannel, List<KeyValuePair<string, ByteString>>> RouteToChannel(string partitionKey, IReadOnlyCollection<KeyValuePair<string, T>> items)
        {
            var opsByChannel = new Dictionary<ICacheNodeChannel, List<KeyValuePair<string, ByteString>>>();

            ICacheNodeChannel lastNode = null;
            List<KeyValuePair<string, ByteString>> lastList = null;
            foreach (var item in items)
            {
                if (item.Key != null)
                {
                    var routeKey = _router.GetRouteKey(partitionKey, item.Key);
                    var node = _hashFunction.Hash(routeKey);
                    if (node == null)
                    {
                        throw new Exception("No node is available to service this operation.");
                    }

                    List<KeyValuePair<string, ByteString>> list;
                    if (node == lastNode)
                    {
                        list = lastList;
                    }
                    else if (!opsByChannel.TryGetValue(node, out list))
                    {
                        list = new List<KeyValuePair<string, ByteString>>();
                        opsByChannel.Add(node, list);
                    }
                    lastNode = node;
                    lastList = list;

                    list.Add(new KeyValuePair<string, ByteString>(item.Key, ByteString.CopyFrom(_codec.Encode(item.Value))));
                }
            }

            return opsByChannel;
        }

        private Dictionary<ICacheNodeChannel, List<string>> RouteToChannel(string partitionKey, IReadOnlyCollection<string> items)
        {
            var opsByChannel = new Dictionary<ICacheNodeChannel, List<string>>();

            ICacheNodeChannel lastNode = null;
            List<string> lastList = null;
            foreach (var item in items)
            {
                if (item != null)
                {
                    var routeKey = _router.GetRouteKey(partitionKey, item);
                    var node = _hashFunction.Hash(routeKey);
                    if (node == null)
                    {
                        throw new Exception("No node is available to service this operation.");
                    }

                    List<string> list;
                    if (node == lastNode)
                    {
                        list = lastList;
                    }
                    else if (!opsByChannel.TryGetValue(node, out list))
                    {
                        list = new List<string>();
                        opsByChannel.Add(node, list);
                    }
                    lastNode = node;
                    lastList = list;

                    list.Add(item);
                }
            }

            return opsByChannel;
        }
    }
}
