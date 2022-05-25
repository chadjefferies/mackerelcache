using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Client.Configuration;

namespace Mackerel.RemoteCache.Client
{
    internal class CacheNodeChannelImpl : ICacheNodeChannel, IEquatable<CacheNodeChannelImpl>
    {
        private readonly ServiceClient _serviceClient;
        private readonly GrpcChannel _channel;
        private readonly CacheClientOptions _config;

        public string Address { get; }

        public CacheNodeChannelImpl(
            CacheClientOptions config,
            string address,
            GrpcChannel channel,
            ServiceClient serviceClient)
        {
            _config = config;
            Address = address;
            _channel = channel;
            _serviceClient = serviceClient;
        }

        public async Task<NodeOperationResult<WriteResult>> DeleteAsync(string partitionKey, string key, CancellationToken token = default)
        {
            try
            {
                var request = new DeleteRequest
                {
                    Key = key,
                    PartitionKey = partitionKey
                };

                var response = await _serviceClient.Cache.DeleteAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<WriteResult>(Address, response.Result, new CacheException(response.Result));
                }

                return new NodeOperationResult<WriteResult>(Address, response.Result);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<WriteResult>(Address, e);
            }
        }

        public async Task<NodeOperationResult<int>> DeleteAsync(string partitionKey, IEnumerable<string> keys, CancellationToken token = default)
        {
            try
            {
                var request = new DeleteManyRequest
                {
                    PartitionKey = partitionKey
                };

                foreach (var key in keys)
                {
                    request.Keys.Add(key);
                }

                var response = await _serviceClient.Cache.DeleteManyAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                return new NodeOperationResult<int>(Address, response.Deleted);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<int>(Address, e);
            }
        }

        public async Task<NodeOperationResult<ByteString>> GetAsync(string partitionKey, string key, CancellationToken token = default)
        {
            try
            {
                var request = new GetRequest
                {
                    Key = key,
                    PartitionKey = partitionKey
                };

                var response = await _serviceClient.Cache.GetAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                return new NodeOperationResult<ByteString>(Address, response.Value);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<ByteString>(Address, e);
            }
        }

        public async Task<NodeOperationResult<IDictionary<string, ByteString>>> GetAsync(string partitionKey, IEnumerable<string> keys, CancellationToken token = default)
        {
            try
            {
                var request = new GetManyRequest
                {
                    PartitionKey = partitionKey
                };

                foreach (var key in keys)
                {
                    request.Keys.Add(key);
                }

                var response = await _serviceClient.Cache.GetManyAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));
                return new NodeOperationResult<IDictionary<string, ByteString>>(Address, response.Entries);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<IDictionary<string, ByteString>>(Address, e);
            }
        }

        public async Task<NodeOperationResult<WriteResult>> PutAsync(string partitionKey, string key, ByteString value, CancellationToken token = default)
        {
            try
            {
                var request = new PutRequest
                {
                    Key = key,
                    Value = value,
                    PartitionKey = partitionKey
                };

                var response = await _serviceClient.Cache.PutAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<WriteResult>(Address, response.Result, new CacheException(response.Result));
                }

                return new NodeOperationResult<WriteResult>(Address, response.Result);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<WriteResult>(Address, e);
            }
        }

        public async Task<NodeOperationResult<WriteResult>> PutAsync(string partitionKey, IEnumerable<KeyValuePair<string, ByteString>> items, CancellationToken token = default)
        {
            try
            {
                var request = new PutManyRequest
                {
                    PartitionKey = partitionKey
                };

                foreach (var item in items)
                {
                    request.Entries.Add(item.Key, item.Value);
                }

                var response = await _serviceClient.Cache.PutManyAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<WriteResult>(Address, response.Result, new CacheException(response.Result));
                }

                return new NodeOperationResult<WriteResult>(Address, response.Result);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<WriteResult>(Address, e);
            }
        }

        public async Task<NodeOperationResult<WriteResult>> PutIfNotExistsAsync(string partitionKey, string key, ByteString value, CancellationToken token = default)
        {
            try
            {
                var request = new PutIfNotExistsRequest
                {
                    Key = key,
                    Value = value,
                    PartitionKey = partitionKey
                };

                var response = await _serviceClient.Cache.PutIfNotExistsAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<WriteResult>(Address, response.Result, new CacheException(response.Result));
                }

                return new NodeOperationResult<WriteResult>(Address, response.Result);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<WriteResult>(Address, e);
            }
        }

        public async Task<NodeOperationResult<WriteResult>> PutIfNotExistsAsync(string partitionKey, IEnumerable<KeyValuePair<string, ByteString>> items, CancellationToken token = default)
        {
            try
            {
                var request = new PutIfNotExistsManyRequest
                {
                    PartitionKey = partitionKey
                };

                foreach (var item in items)
                {
                    request.Entries.Add(item.Key, item.Value);
                }

                var response = await _serviceClient.Cache.PutIfNotExistsManyAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<WriteResult>(Address, response.Result, new CacheException(response.Result));
                }

                return new NodeOperationResult<WriteResult>(Address, response.Result);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<WriteResult>(Address, e);
            }
        }

        public async Task<NodeOperationResult<WriteResult>> PutIfExistsAsync(string partitionKey, string key, ByteString value, CancellationToken token = default)
        {
            try
            {
                var request = new PutIfExistsRequest
                {
                    Key = key,
                    Value = value,
                    PartitionKey = partitionKey
                };

                var response = await _serviceClient.Cache.PutIfExistsAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<WriteResult>(Address, response.Result, new CacheException(response.Result));
                }

                return new NodeOperationResult<WriteResult>(Address, response.Result);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<WriteResult>(Address, e);
            }
        }

        public async Task<NodeOperationResult<WriteResult>> PutIfExistsAsync(string partitionKey, IEnumerable<KeyValuePair<string, ByteString>> items, CancellationToken token = default)
        {
            try
            {
                var request = new PutIfExistsManyRequest
                {
                    PartitionKey = partitionKey
                };

                foreach (var item in items)
                {
                    request.Entries.Add(item.Key, item.Value);
                }

                var response = await _serviceClient.Cache.PutIfExistsManyAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<WriteResult>(Address, response.Result, new CacheException(response.Result));
                }

                return new NodeOperationResult<WriteResult>(Address, response.Result);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<WriteResult>(Address, e);
            }
        }

        public async Task<NodeOperationResult<WriteResult>> DeletePartitionAsync(string partitionKey, CancellationToken token = default)
        {
            try
            {
                var request = new DeletePartitionRequest
                {
                    PartitionKey = partitionKey
                };

                var response = await _serviceClient.Cache.DeletePartitionAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<WriteResult>(Address, response.Result, new CacheException(response.Result));
                }

                return new NodeOperationResult<WriteResult>(Address, response.Result);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<WriteResult>(Address, e);
            }
        }

        public async Task<NodeOperationResult> FlushPartitionAsync(string partitionKey, CancellationToken token = default)
        {
            try
            {
                var request = new FlushPartitionRequest
                {
                    PartitionKey = partitionKey
                };

                var response = await _serviceClient.Cache.FlushPartitionAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                return new NodeOperationResult(Address);
            }
            catch (Exception e)
            {
                return new NodeOperationResult(Address, e);
            }
        }

        public async Task<NodeOperationResult> FlushAllAsync(CancellationToken token = default)
        {
            try
            {
                var request = new FlushAllRequest();

                var response = await _serviceClient.Cache.FlushAllAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                return new NodeOperationResult(Address);
            }
            catch (Exception e)
            {
                return new NodeOperationResult(Address, e);
            }
        }

        public async Task<NodeOperationResult> InvokeGCAsync(CancellationToken token = default)
        {
            try
            {
                var request = new InvokeGCRequest();
                var response = await _serviceClient.Maintenance.InvokeGCAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                return new NodeOperationResult(Address);
            }
            catch (Exception e)
            {
                return new NodeOperationResult(Address, e);
            }
        }

        public async Task<NodeOperationResult<CacheStats>> GetStatsAsync(CancellationToken token = default)
        {
            try
            {
                var request = new GetStatsRequest();
                var response = await _serviceClient.Maintenance.GetStatsAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                return new NodeOperationResult<CacheStats>(Address, response);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<CacheStats>(Address, e);
            }
        }

        public async Task<NodeOperationResult<PartitionStats>> GetPartitionStatsAsync(string partitionKey, CancellationToken token = default)
        {
            try
            {
                var request = new GetPartitionStatsRequest
                {
                    PartitionKey = partitionKey
                };

                var response = await _serviceClient.Maintenance.GetPartitionStatsAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                return new NodeOperationResult<PartitionStats>(Address, response);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<PartitionStats>(Address, e);
            }
        }

        public AsyncServerStreamingCall<ScanPartitionsResponse> ScanPartitionsAsync(string pattern, int count, CancellationToken token = default)
        {
            var request = new ScanPartitionsRequest
            {
                Pattern = pattern,
                Count = count
            };

            return _serviceClient.Cache.ScanPartitions(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));
        }

        public AsyncServerStreamingCall<ScanKeysResponse> ScanKeysAsync(string partitionKey, string pattern, int count, int offset, CancellationToken token = default)
        {
            var request = new ScanKeysRequest
            {
                PartitionKey = partitionKey,
                Pattern = pattern,
                Count = count,
                Offset = offset
            };

            return _serviceClient.Cache.ScanKeys(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));
        }

        public async Task<NodeOperationResult<WriteResult>> PutPartitionAsync(string partitionKey, TimeSpan expiration, ExpirationType expirationType, bool persist, EvictionPolicy evictionPolicy, long maxCacheSize, CancellationToken token = default)
        {
            try
            {
                var request = new PutPartitionRequest
                {
                    Expiration = Duration.FromTimeSpan(expiration),
                    ExpirationType = expirationType,
                    PartitionKey = partitionKey,
                    Persist = persist,
                    EvictionPolicy = evictionPolicy,
                    MaxCacheSize = maxCacheSize
                };
                var response = await _serviceClient.Cache.PutPartitionAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<WriteResult>(Address, response.Result, new CacheException(response.Result));
                }

                return new NodeOperationResult<WriteResult>(Address, response.Result);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<WriteResult>(Address, e);
            }
        }

        public async Task<NodeOperationResult<CacheConfiguration>> GetConfAsync(CancellationToken token = default)
        {
            try
            {
                var request = new GetConfRequest();

                var response = await _serviceClient.Maintenance.GetConfAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                return new NodeOperationResult<CacheConfiguration>(Address, response);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<CacheConfiguration>(Address, e);
            }
        }

        public async Task<NodeOperationResult<string>> PingAsync(CancellationToken token = default)
        {
            try
            {
                var request = new PingRequest();

                var response = await _serviceClient.Maintenance.PingAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                return new NodeOperationResult<string>(Address, response.Result);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<string>(Address, e);
            }
        }

        public AsyncDuplexStreamingCall<WatchRequest, WatchResponse> WatchAsync(CancellationToken token = default)
        {
            return _serviceClient.Watch.Watch(GetCallOptions(null, _config.SessionTimeoutMilliseconds, token));
        }

        public async Task<NodeOperationResult<WriteResult>> TouchAsync(string partitionKey, string key, CancellationToken token = default)
        {
            try
            {
                var request = new TouchRequest
                {
                    Key = key,
                    PartitionKey = partitionKey
                };

                var response = await _serviceClient.Cache.TouchAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<WriteResult>(Address, response.Result, new CacheException(response.Result));
                }

                return new NodeOperationResult<WriteResult>(Address, response.Result);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<WriteResult>(Address, e);
            }
        }

        public async Task<NodeOperationResult<int>> TouchAsync(string partitionKey, IEnumerable<string> keys, CancellationToken token = default)
        {
            try
            {
                var request = new TouchManyRequest
                {
                    PartitionKey = partitionKey
                };

                foreach (var key in keys)
                {
                    request.Keys.Add(key);
                }

                var response = await _serviceClient.Cache.TouchManyAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                return new NodeOperationResult<int>(Address, response.Touched);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<int>(Address, e);
            }
        }

        public async Task<NodeOperationResult<long>> TtlAsync(string partitionKey, string key, CancellationToken token = default)
        {
            try
            {
                var request = new TtlRequest
                {
                    Key = key,
                    PartitionKey = partitionKey
                };

                var response = await _serviceClient.Cache.TtlAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                return new NodeOperationResult<long>(Address, response.ValueMs);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<long>(Address, e);
            }
        }

        public async Task<NodeOperationResult<IReadOnlyList<KeyValuePair<string, long>>>> TtlAsync(string partitionKey, IEnumerable<string> keys, CancellationToken token = default)
        {
            try
            {
                var request = new TtlManyRequest
                {
                    PartitionKey = partitionKey
                };

                foreach (var key in keys)
                {
                    request.Keys.Add(key);
                }

                var response = await _serviceClient.Cache.TtlManyAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));
                var entries = new List<KeyValuePair<string, long>>(response.Entries.Count);

                foreach (var item in response.Entries)
                {
                    entries.Add(new KeyValuePair<string, long>(item.Key, item.Value));
                }
                return new NodeOperationResult<IReadOnlyList<KeyValuePair<string, long>>>(Address, entries);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<IReadOnlyList<KeyValuePair<string, long>>>(Address, e);
            }
        }

        public async Task<NodeOperationResult<long>> IncrementAsync(string partitionKey, string key, CancellationToken token = default)
        {
            try
            {
                var request = new IncrementRequest
                {
                    Key = key,
                    PartitionKey = partitionKey
                };

                var response = await _serviceClient.Cache.IncrementAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<long>(Address, response.Value, new CacheException(response.Result));
                }

                return new NodeOperationResult<long>(Address, response.Value);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<long>(Address, e);
            }
        }

        public async Task<NodeOperationResult<long>> IncrementByAsync(string partitionKey, string key, long value, CancellationToken token = default)
        {
            try
            {
                var request = new IncrementByRequest
                {
                    Key = key,
                    PartitionKey = partitionKey,
                    Value = value
                };

                var response = await _serviceClient.Cache.IncrementByAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<long>(Address, response.Value, new CacheException(response.Result));
                }

                return new NodeOperationResult<long>(Address, response.Value);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<long>(Address, e);
            }
        }

        public async Task<NodeOperationResult<long>> DecrementAsync(string partitionKey, string key, CancellationToken token = default)
        {
            try
            {
                var request = new DecrementRequest
                {
                    Key = key,
                    PartitionKey = partitionKey
                };

                var response = await _serviceClient.Cache.DecrementAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<long>(Address, response.Value, new CacheException(response.Result));
                }

                return new NodeOperationResult<long>(Address, response.Value);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<long>(Address, e);
            }
        }

        public async Task<NodeOperationResult<long>> DecrementByAsync(string partitionKey, string key, long value, CancellationToken token = default)
        {
            try
            {
                var request = new DecrementByRequest
                {
                    Key = key,
                    PartitionKey = partitionKey,
                    Value = value
                };

                var response = await _serviceClient.Cache.DecrementByAsync(request, GetCallOptions(_config.TimeoutMilliseconds, null, token));

                if (response.Result != WriteResult.Success)
                {
                    return new NodeOperationResult<long>(Address, response.Value, new CacheException(response.Result));
                }

                return new NodeOperationResult<long>(Address, response.Value);
            }
            catch (Exception e)
            {
                return new NodeOperationResult<long>(Address, e);
            }
        }

        public override bool Equals(object obj) => Equals(obj as CacheNodeChannelImpl);

        public bool Equals(CacheNodeChannelImpl other)
        {
            if (other is null) return false;
            return other.Address == Address;
        }

        public override int GetHashCode() => Address.GetHashCode();

        private static CallOptions GetCallOptions(int? timeoutMilliseconds, int? sessionTimeoutMilliseconds, CancellationToken token)
        {
            DateTime? deadline = null;
            if (timeoutMilliseconds.HasValue)
            {
                // TODO: system clock?
                deadline = DateTime.UtcNow.AddMilliseconds(timeoutMilliseconds.Value);
            }

            var callOptions = new CallOptions(
                deadline: deadline,
                cancellationToken: token);

            if (sessionTimeoutMilliseconds.HasValue)
            {
                return callOptions.WithHeaders(new Metadata
                {
                    new Metadata.Entry("session_timeout", sessionTimeoutMilliseconds.Value.ToString())
                });
            }

            return callOptions;
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await _channel.ShutdownAsync();
                _channel.Dispose();
            }
            catch { }
        }
    }
}
