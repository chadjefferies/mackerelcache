using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Eviction;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Statistics;
using Mackerel.RemoteCache.Server.Util;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Internal;
using Quartz;
using static Mackerel.RemoteCache.Api.V1.RemoteCacheService;

namespace Mackerel.RemoteCache.Server.Rpc
{
    public class RemoteCacheServiceHandler : RemoteCacheServiceBase
    {
        private readonly MemoryStore _cache;
        private readonly ISystemClock _systemClock;
        private readonly ISchedulerFactory _schedulerFactory;
        private readonly IHostApplicationLifetime _hostLifetime;

        public RemoteCacheServiceHandler(MemoryStore cache, ISystemClock systemClock, ISchedulerFactory schedulerFactory, IHostApplicationLifetime hostLifetime)
        {
            _cache = cache;
            _systemClock = systemClock;
            _schedulerFactory = schedulerFactory;
            _hostLifetime = hostLifetime;
        }

        public override Task<GetResponse> Get(GetRequest request, ServerCallContext context)
        {
            CacheValue data = _cache.Get(
                request.PartitionKey,
                request.Key,
                _systemClock.UtcNow.UtcDateTime);
            return data is null ? StaticResponse.EmptyGet : Task.FromResult(new GetResponse
            {
                Value = data
            });
        }

        public override Task<GetManyResponse> GetMany(GetManyRequest request, ServerCallContext context)
        {
            using var dataBlock = _cache.Get(
                request.PartitionKey,
                request.Keys,
                _systemClock.UtcNow.UtcDateTime);

            var response = new GetManyResponse();
            for (int i = 0; i < dataBlock.Data.Length; i++)
            {
                var outgoing = dataBlock.Data[i];
                response.Entries.Add(outgoing.Key, outgoing.Value);
            }

            return Task.FromResult(response);
        }

        public override Task<PutResponse> Put(PutRequest request, ServerCallContext context)
        {
            var timestamp = _systemClock.UtcNow.UtcDateTime;
            WriteResult result = _cache.Put(
                request.PartitionKey,
                request.Key,
                request.Value,
                timestamp);
            return Task.FromResult(new PutResponse
            {
                Result = result
            });
        }

        public override Task<PutManyResponse> PutMany(PutManyRequest request, ServerCallContext context)
        {
            var timestamp = _systemClock.UtcNow.UtcDateTime;
            WriteResult result = _cache.Put(
                request.PartitionKey,
                request.Entries,
                timestamp);
            return Task.FromResult(new PutManyResponse
            {
                Result = result
            });
        }

        public override Task<PutIfExistsResponse> PutIfExists(PutIfExistsRequest request, ServerCallContext context)
        {
            var timestamp = _systemClock.UtcNow.UtcDateTime;
            WriteResult result = _cache.PutIfExists(
                request.PartitionKey,
                request.Key,
                request.Value,
                timestamp);
            return Task.FromResult(new PutIfExistsResponse
            {
                Result = result
            });
        }

        public override Task<PutIfExistsManyResponse> PutIfExistsMany(PutIfExistsManyRequest request, ServerCallContext context)
        {
            var timestamp = _systemClock.UtcNow.UtcDateTime;
            WriteResult result = _cache.PutIfExists(
                request.PartitionKey,
                request.Entries,
                timestamp);
            return Task.FromResult(new PutIfExistsManyResponse
            {
                Result = result
            });
        }

        public override Task<PutIfNotExistsResponse> PutIfNotExists(PutIfNotExistsRequest request, ServerCallContext context)
        {
            var timestamp = _systemClock.UtcNow.UtcDateTime;
            WriteResult result = _cache.PutIfNotExists(
                request.PartitionKey,
                request.Key,
                request.Value,
                timestamp);
            return Task.FromResult(new PutIfNotExistsResponse
            {
                Result = result
            });
        }

        public override Task<PutIfNotExistsManyResponse> PutIfNotExistsMany(PutIfNotExistsManyRequest request, ServerCallContext context)
        {
            var timestamp = _systemClock.UtcNow.UtcDateTime;
            WriteResult result = _cache.PutIfNotExists(
                request.PartitionKey,
                request.Entries,
                timestamp);
            return Task.FromResult(new PutIfNotExistsManyResponse
            {
                Result = result
            });
        }

        public override Task<DeleteResponse> Delete(DeleteRequest request, ServerCallContext context)
        {
            WriteResult result = _cache.Delete(
                request.PartitionKey,
                request.Key,
                _systemClock.UtcNow.UtcDateTime);
            return Task.FromResult(new DeleteResponse
            {
                Result = result
            });
        }

        public override Task<DeleteManyResponse> DeleteMany(DeleteManyRequest request, ServerCallContext context)
        {
            var result = _cache.Delete(
                 request.PartitionKey,
                 request.Keys,
                 _systemClock.UtcNow.UtcDateTime);

            return Task.FromResult(new DeleteManyResponse
            {
                Deleted = result
            });
        }

        public override Task<DeletePartitionResponse> DeletePartition(DeletePartitionRequest request, ServerCallContext context)
        {
            WriteResult result = _cache.DeletePartition(request.PartitionKey);
            return Task.FromResult(new DeletePartitionResponse
            {
                Result = result
            });
        }

        public override Task<FlushPartitionResponse> FlushPartition(FlushPartitionRequest request, ServerCallContext context)
        {
            _cache.FlushPartition(request.PartitionKey);
            return StaticResponse.FlushPartition;
        }

        public override Task<FlushAllResponse> FlushAll(FlushAllRequest request, ServerCallContext context)
        {
            _cache.FlushAll();
            return StaticResponse.FlushAll;
        }

        public override async Task<PutPartitionResponse> PutPartition(PutPartitionRequest request, ServerCallContext context)
        {
            var metadata = new PartitionMetadata(
                    _systemClock.UtcNow.UtcDateTime.Ticks,
                    request.Expiration.ToTimeSpan().Ticks,
                    request.ExpirationType == ExpirationType.Absolute,
                    request.Persist,
                    request.EvictionPolicy,
                    request.MaxCacheSize);
            WriteResult result = await _cache.PutPartition(
                request.PartitionKey,
                metadata,
                _systemClock.UtcNow.UtcDateTime);

            if (result == WriteResult.Success && metadata.EvictionPolicy != EvictionPolicy.NoEviction)
            {
                // In case the size on the partition decreased,
                // execute the compaction job to shrink it down to size
                var jobKey = new JobKey(nameof(PartitionCompactionJob));
                IDictionary<string, object> jobData = new Dictionary<string, object> { { "partitionKey", request.PartitionKey } };
                var scheduler = await _schedulerFactory.GetScheduler(context.CancellationToken);
                await scheduler.TriggerJob(jobKey, new JobDataMap(jobData), context.CancellationToken);
            }

            return new PutPartitionResponse
            {
                Result = result
            };
        }

        public override async Task ScanPartitions(ScanPartitionsRequest request, IServerStreamWriter<ScanPartitionsResponse> responseStream, ServerCallContext context)
        {
            using var callSessionToken = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken, _hostLifetime.ApplicationStopping);
            var regex = request.Pattern.FromGlobPattern();
            int resultsWritten = 0;
            if (request.Count != 0)
            {
                foreach (var partition in _cache)
                {
                    if (callSessionToken.IsCancellationRequested) break;
                    string partitionKey = partition.Key;
                    if (regex.Match(partitionKey).Success)
                    {
                        var response = new ScanPartitionsResponse
                        {
                            Stats = new PartitionStats()
                            {
                                CurrentItemCount = partition.Value.Stats.CurrentItemCount,
                                HitRate = RuntimeStatistics.CalculateHitRate(partition.Value.Stats.TotalHits, partition.Value.Stats.TotalMisses),
                                Expiration = TimeSpan.FromTicks(partition.Value.Metadata.ExpirationTicks).ToDuration(),
                                ExpirationType = partition.Value.Metadata.IsAbsoluteExpiration ? ExpirationType.Absolute : ExpirationType.Sliding,
                                LastHitDate = new DateTime(partition.Value.Stats.LastHit, DateTimeKind.Utc).ToTimestamp(),
                                PartitionKey = partitionKey,
                                TotalEvictionCount = partition.Value.Stats.TotalEvictionCount,
                                TotalExpiredCount = partition.Value.Stats.TotalExpiredCount,
                                TotalHits = partition.Value.Stats.TotalHits,
                                TotalMisses = partition.Value.Stats.TotalMisses,
                                Persisted = partition.Value.Metadata.IsPersisted,
                                TotalCacheSize = partition.Value.Stats.TotalCacheSize,
                                EvictionPolicy = partition.Value.Metadata.EvictionPolicy,
                                MaxCacheSize = partition.Value.Metadata.MaxCacheSize,
                                CreateDate = new DateTime(partition.Value.Metadata.CreateDate, DateTimeKind.Utc).ToTimestamp(),

                            }
                        };

                        await responseStream.WriteAsync(response);
                        resultsWritten++;
                        if (request.Count > -1 && resultsWritten == request.Count) break;
                    }
                }
            }
        }

        public override async Task ScanKeys(ScanKeysRequest request, IServerStreamWriter<ScanKeysResponse> responseStream, ServerCallContext context)
        {
            int index = 0;
            using var callSessionToken = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken, _hostLifetime.ApplicationStopping);
            if (request.Count > 0)
            {
                var partition = _cache.GetPartition(request.PartitionKey);
                if (partition.Value != null)
                {
                    var regex = request.Pattern.FromGlobPattern();
                    int resultsWritten = 0;

                    // TODO: Collection modified exceptions
                    foreach (var entry in partition.Value)
                    {
                        if (callSessionToken.IsCancellationRequested) break;
                        if (index >= request.Offset)
                        {
                            string key = entry.Key;
                            if (regex.Match(key).Success)
                            {
                                // checks if values are expired
                                var value = partition.Value.Get(entry.Key, _systemClock.UtcNow.UtcDateTime, false, false);
                                if (value != default)
                                {
                                    var response = new ScanKeysResponse
                                    {
                                        Key = key,
                                        Value = value,
                                        Index = index
                                    };

                                    await responseStream.WriteAsync(response);
                                    resultsWritten++;
                                    if (request.Count > -1 && resultsWritten == request.Count) break;
                                }
                            }
                        }
                        index++;
                    }
                }
            }
        }

        public override Task<TouchResponse> Touch(TouchRequest request, ServerCallContext context)
        {
            var result = _cache.Touch(
                request.PartitionKey,
                request.Key,
                _systemClock.UtcNow.UtcDateTime);
            return Task.FromResult(new TouchResponse
            {
                Result = result
            });
        }

        public override Task<TouchManyResponse> TouchMany(TouchManyRequest request, ServerCallContext context)
        {
            var result = _cache.Touch(
                 request.PartitionKey,
                 request.Keys,
                 _systemClock.UtcNow.UtcDateTime);
            return Task.FromResult(new TouchManyResponse
            {
                Touched = result
            });
        }

        public override Task<TtlResponse> Ttl(TtlRequest request, ServerCallContext context)
        {
            var data = _cache.Ttl(
                request.PartitionKey,
                request.Key,
                _systemClock.UtcNow.UtcDateTime);
            return Task.FromResult(new TtlResponse
            {
                ValueMs = data / TimeSpan.TicksPerMillisecond
            });
        }

        public override Task<TtlManyResponse> TtlMany(TtlManyRequest request, ServerCallContext context)
        {
            var data = _cache.Ttl(
                request.PartitionKey,
                request.Keys,
                _systemClock.UtcNow.UtcDateTime);

            var response = new TtlManyResponse();
            for (int i = 0; i < data.Count; i++)
            {
                var outgoing = data[i];
                response.Entries.Add(outgoing.Key, outgoing.Value / TimeSpan.TicksPerMillisecond);
            }

            return Task.FromResult(response);
        }

        public override Task<IncrementResponse> Increment(IncrementRequest request, ServerCallContext context)
        {
            WriteResult result = _cache.TryIncrementValue(
                request.PartitionKey,
                request.Key,
                1,
                _systemClock.UtcNow.UtcDateTime,
                out var value);
            return Task.FromResult(new IncrementResponse
            {
                Value = value,
                Result = result
            });
        }

        public override Task<IncrementByResponse> IncrementBy(IncrementByRequest request, ServerCallContext context)
        {
            WriteResult result = _cache.TryIncrementValue(
                request.PartitionKey,
                request.Key,
                request.Value,
                _systemClock.UtcNow.UtcDateTime,
                out var value);
            return Task.FromResult(new IncrementByResponse
            {
                Value = value,
                Result = result
            });
        }

        public override Task<DecrementResponse> Decrement(DecrementRequest request, ServerCallContext context)
        {
            WriteResult result = _cache.TryIncrementValue(
                request.PartitionKey,
                request.Key,
                -1,
                _systemClock.UtcNow.UtcDateTime,
                out var value);
            return Task.FromResult(new DecrementResponse
            {
                Value = value,
                Result = result
            });
        }

        public override Task<DecrementByResponse> DecrementBy(DecrementByRequest request, ServerCallContext context)
        {
            WriteResult result = _cache.TryIncrementValue(
                request.PartitionKey,
                request.Key,
                -request.Value,
                _systemClock.UtcNow.UtcDateTime,
                out var value);
            return Task.FromResult(new DecrementByResponse
            {
                Value = value,
                Result = result
            });
        }
    }
}
