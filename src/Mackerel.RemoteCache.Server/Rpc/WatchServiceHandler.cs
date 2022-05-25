using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Watch;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Internal;
using static Mackerel.RemoteCache.Api.V1.WatchService;

namespace Mackerel.RemoteCache.Server.Rpc
{
    public class WatchServiceHandler : WatchServiceBase
    {
        private static long _watcherId;

        private readonly MemoryStore _cache;
        private readonly ISystemClock _systemClock;
        private readonly IHostApplicationLifetime _hostLifetime;

        public WatchServiceHandler(MemoryStore cache, ISystemClock systemClock, IHostApplicationLifetime hostLifetime)
        {
            _cache = cache;
            _systemClock = systemClock;
            _hostLifetime = hostLifetime;
        }

        public override async Task Watch(IAsyncStreamReader<WatchRequest> requestStream, IServerStreamWriter<WatchResponse> responseStream, ServerCallContext context)
        {
            var watcherId = new CacheKey(Interlocked.Increment(ref _watcherId));
            using var watcher = new WatcherChannel(watcherId);
            _cache.Stats.IncrementWatchStreams();

            try
            {
                var sessionTimeout = context.RequestHeaders.GetSessionTimeout();
                using var sessionTimeoutToken = new CancellationTokenSource(sessionTimeout);
                using var callSessionToken = CancellationTokenSource.CreateLinkedTokenSource(
                    sessionTimeoutToken.Token, context.CancellationToken, _hostLifetime.ApplicationStopping);

                if (await requestStream.MoveNext(callSessionToken.Token))
                {
                    HandleRequestUnion();

                    var responseStreamTask = Task.Run(async () =>
                    {
                        await foreach (var changeEvent in watcher.ReadAsync(callSessionToken.Token))
                        {
                            var watchResponse = new WatchResponse
                            {
                                WatchId = changeEvent.WatchId,
                                PartitionKey = changeEvent.PartitionKey,
                                Key = changeEvent.Key,
                                WatchEventType = changeEvent.EventType.ToWatchEventType()
                            };
                            if (changeEvent.Value != default)
                            {
                                watchResponse.Value = ByteString.CopyFrom(changeEvent.Value, 0, changeEvent.Value.Length);
                            }
                            
                            _cache.Stats.IncrementWatchEvents();
                            await responseStream.WriteAsync(watchResponse);
                        }
                    });

                    while (await requestStream.MoveNext(callSessionToken.Token))
                    {
                        HandleRequestUnion();
                    }

                    await responseStreamTask;

                    void HandleRequestUnion()
                    {
                        switch (requestStream.Current.RequestUnionCase)
                        {
                            case WatchRequest.RequestUnionOneofCase.CreateRequest:
                                if (_cache.PutWatchPredicate(watcher, requestStream.Current.CreateRequest.WatchId, requestStream.Current.CreateRequest.PartitionKey, requestStream.Current.CreateRequest.Key, requestStream.Current.CreateRequest.Filters, _systemClock.UtcNow.UtcDateTime))
                                {
                                    watcher.IncrementPredicates();
                                    _cache.Stats.IncrementWatches();
                                }
                                goto case WatchRequest.RequestUnionOneofCase.KeepAliveRequest;
                            case WatchRequest.RequestUnionOneofCase.KeepAliveRequest:
                                sessionTimeoutToken.CancelAfter(sessionTimeout);
                                break;
                            case WatchRequest.RequestUnionOneofCase.CancelRequest:
                                if (_cache.DeleteWatchPredicate(watcher, requestStream.Current.CancelRequest.WatchId, requestStream.Current.CancelRequest.PartitionKey))
                                {
                                    watcher.DecrementPredicates();
                                    _cache.Stats.DecrementWatches();
                                }
                                break;
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                throw new RpcException(new Status(StatusCode.Cancelled, "Watch session has been cancelled."));
            }
            finally
            {
                _cache.Stats.DecrementWatchesBy(watcher.Predicates);
                _cache.Stats.DecrementWatchStreams();
            }
        }
    }
}
