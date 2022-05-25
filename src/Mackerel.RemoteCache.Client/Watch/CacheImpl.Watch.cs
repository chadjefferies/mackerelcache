using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Client.Util;
using Mackerel.RemoteCache.Client.Watch;

namespace Mackerel.RemoteCache.Client
{
    /// <summary>
    /// A typed cache reference used to access a remote cache.
    /// </summary>
    public partial class CacheImpl<T> : ICache<T>
    {
        private ConcurrentDictionary<string, AsyncDuplexStreamingCall<WatchRequest, WatchResponse>> _watchStreams = new ConcurrentDictionary<string, AsyncDuplexStreamingCall<WatchRequest, WatchResponse>>();
        private ConcurrentDictionary<string, Task> _streamReaders = new ConcurrentDictionary<string, Task>();
        private Dictionary<string, WatchReference<T>> _watchers;
        private SemaphoreSlim _bootstrapSemaphore;
        private CancellationTokenSource _watcherToken = new CancellationTokenSource();
        private Timer _keepAliveTimer;
        private volatile bool _isInitiated;

        public CancellationToken WatchToken => _watcherToken.Token;

        public Task WatchAsync(string watchId, string partitionKey, Action<WatchEvent<T>> handler, CancellationToken token = default)
        {
            return WatchAsync(watchId, partitionKey, default, Array.Empty<WatchEventType>(), handler, token);
        }

        public Task WatchAsync(string watchId, string partitionKey, IReadOnlyList<WatchEventType> filters, Action<WatchEvent<T>> handler, CancellationToken token = default)
        {
            return WatchAsync(watchId, partitionKey, default, filters, handler, token);
        }

        public Task WatchAsync(string watchId, string partitionKey, string key, Action<WatchEvent<T>> handler, CancellationToken token = default)
        {
            return WatchAsync(watchId, partitionKey, key, Array.Empty<WatchEventType>(), handler, token);
        }

        public async Task WatchAsync(string watchId, string partitionKey, string key, IReadOnlyList<WatchEventType> filters, Action<WatchEvent<T>> handler, CancellationToken token = default)
        {
            CachePreconditions.CheckNotNull(watchId, nameof(watchId));
            CachePreconditions.CheckNotNull(partitionKey, nameof(partitionKey));

            _watchStreams ??= new ConcurrentDictionary<string, AsyncDuplexStreamingCall<WatchRequest, WatchResponse>>();
            _streamReaders ??= new ConcurrentDictionary<string, Task>();
            _watchers ??= new Dictionary<string, WatchReference<T>>();
            _bootstrapSemaphore ??= new SemaphoreSlim(1, 1);
            _watcherToken ??= new CancellationTokenSource();

            var watchRequest = new WatchCreateRequest
            {
                WatchId = watchId,
                PartitionKey = partitionKey
            };
            if (key != null) watchRequest.Key = key;
            foreach (var item in filters)
            {
                watchRequest.Filters.Add(item);
            }

            var watcher = new WatchReference<T>(watchRequest, handler);
            AddWatcher(watcher);
            await BootstrapAsync().ConfigureAwait(false);

            var writeTasks = new Task[_watchStreams.Count];
            var i = 0;
            foreach (var watchStream in _watchStreams)
            {
                var request = new WatchRequest
                {
                    CreateRequest = watchRequest
                };

                writeTasks[i++] = watchStream.Value.RequestStream.WriteAsync(request);
            }

            await Task.WhenAll(writeTasks).ConfigureAwait(false);
        }

        public async Task CancelAsync(string watchId, string partitionKey)
        {
            var watchStreamTasks = new Task[_watchStreams.Count];
            var i = 0;
            foreach (var watchStream in _watchStreams)
            {
                watchStreamTasks[i++] = watchStream.Value.RequestStream.WriteAsync(new WatchRequest
                {
                    CancelRequest = new WatchCancelRequest
                    {
                        WatchId = watchId,
                        PartitionKey = partitionKey
                    }
                });
            }

            await Task.WhenAll(watchStreamTasks).ConfigureAwait(false);
        }

        private void AddWatcher(WatchReference<T> watcher)
        {
            lock (_watchers)
            {
                _watchers[watcher.CreateRequest.WatchId] = watcher;
            }
        }

        private async Task BootstrapAsync()
        {
            if (await _bootstrapSemaphore.WaitAsync(0).ConfigureAwait(false))
            {
                try
                {
                    if (!_isInitiated)
                    {
                        _watcherToken.Cancel();
                        _watcherToken.Dispose();
                        await CompleteStreamsAsync().ConfigureAwait(false);
                        _watcherToken = new CancellationTokenSource();

                        foreach (var streamReader in _streamReaders)
                        {
                            await streamReader.Value.ConfigureAwait(false);
                            streamReader.Value.Dispose();
                        }
                        _streamReaders.Clear();

                        if (_keepAliveTimer == null)
                        {
                            _keepAliveTimer = new Timer(SendKeepAliveAsync, null, Options.SessionTimeoutMilliseconds, Options.SessionTimeoutMilliseconds);
                        }

                        _isInitiated = true;
                    }

                    if (InitiateDuplexStreams() > 0)
                    {
                        foreach (var stream in _watchStreams)
                        {
                            if (!_streamReaders.TryGetValue(stream.Key, out var _))
                            {
                                _streamReaders[stream.Key] = Task.Run(() => ReadFromStreamAsync(stream.Value.ResponseStream)).ContinueWith(HandleErrors, TaskContinuationOptions.OnlyOnFaulted);
                            }
                        }

                        foreach (var watcher in _watchers)
                        {
                            var writeTasks = new Task[_watchStreams.Count];
                            var i = 0;
                            foreach (var watchStream in _watchStreams)
                            {
                                var request = new WatchRequest
                                {
                                    CreateRequest = watcher.Value.CreateRequest
                                };
                                writeTasks[i++] = watchStream.Value.RequestStream.WriteAsync(request);
                            }

                            await Task.WhenAll(writeTasks).ConfigureAwait(false);
                        }
                    }
                }
                finally
                {
                    _bootstrapSemaphore.Release();
                }
            }
        }

        private int InitiateDuplexStreams()
        {
            int watchStreamsAdded = 0;
            foreach (var watcher in _watchers)
            {
                var routeKey = _router.GetRouteKey(watcher.Value.CreateRequest.PartitionKey, watcher.Value.CreateRequest.Key);
                if (string.IsNullOrEmpty(routeKey))
                {
                    // routeKey is null. Could be the router is key specific 
                    // and the watch request is for the entire partition.
                    // In that case, establish watch streams to each node.
                    var nodes = _connection.GetNodes();
                    for (int i = 0; i < nodes.Count; i++)
                    {
                        if (!_watchStreams.TryGetValue(nodes[i].Address, out var _))
                        {
                            _watchStreams[nodes[i].Address] = nodes[i].WatchAsync(_watcherToken.Token);
                            watchStreamsAdded++;
                        }
                    }
                }
                else
                {
                    var node = _hashFunction.Hash(routeKey);
                    if (!_watchStreams.TryGetValue(node.Address, out var _))
                    {
                        _watchStreams[node.Address] = node.WatchAsync(_watcherToken.Token);
                        watchStreamsAdded++;
                    }
                }
            }

            return watchStreamsAdded;
        }

        private async Task ReadFromStreamAsync(IAsyncStreamReader<WatchResponse> stream)
        {
            while (await stream.MoveNext(_watcherToken.Token).ConfigureAwait(false))
            {
                var message = stream.Current;
                if (_watchers.TryGetValue(message.WatchId, out var watcher))
                {
                    var valDecoded = _codec.Decode(message.Value.Span, message.Value.IsEmpty);
                    var watchEvent = new WatchEvent<T>(message.WatchId, message.PartitionKey, message.Key, valDecoded, message.WatchEventType);
                    try
                    {
                        watcher.Handler(watchEvent);
                    }
                    catch (Exception e)
                    {
                        await HandleErrors(e).ConfigureAwait(false);
                    }
                    //ThreadPool.QueueUserWorkItem(_messageHandlerCallback, messageHandler);
                }
            }
        }

        private async void SendKeepAliveAsync(object state)
        {
            try
            {
                var keepAliveTasks = new Task[_watchStreams.Count];
                var i = 0;
                foreach (var watchStream in _watchStreams)
                {
                    keepAliveTasks[i++] = watchStream.Value.RequestStream.WriteAsync(new WatchRequest
                    {
                        KeepAliveRequest = new WatchKeepAliveRequest()
                    });
                }

                await Task.WhenAll(keepAliveTasks).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _ = HandleErrors(e);
            }
        }

        private async Task CompleteStreamsAsync()
        {
            var watchStreamTasks = new Task[_watchStreams.Count];
            var i = 0;
            foreach (var watchStream in _watchStreams)
            {
                watchStreamTasks[i++] = watchStream.Value.RequestStream.CompleteAsync();
            }

            await Task.WhenAll(watchStreamTasks).ConfigureAwait(false);
            foreach (var watchStream in _watchStreams)
            {
                watchStream.Value.Dispose();
            }
            _watchStreams.Clear();
        }

        private Task HandleErrors(Task t) => HandleErrors(t.Exception);

        private async Task HandleErrors(Exception e)
        {
            if ((e.InnerException ?? e) is RpcException re && re.StatusCode == StatusCode.Cancelled) return;
            try
            {
                _connection.ErrorHandler?.Invoke(e);
            }
            catch { }

            _isInitiated = false;
            await BootstrapAsync().ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await CompleteStreamsAsync().ConfigureAwait(false);
                if (!_watcherToken.IsCancellationRequested)
                {
                    _watcherToken.Cancel();
                }
                _watcherToken.Dispose();
                _bootstrapSemaphore?.Dispose();
                if (_keepAliveTimer != null)
                {
                    await _keepAliveTimer.DisposeAsync().ConfigureAwait(false);
                }

                foreach (var streamReader in _streamReaders)
                {
                    await streamReader.Value.ConfigureAwait(false);
                    streamReader.Value.Dispose();
                }
            }
            catch { }
        }
    }
}
