using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Grpc.Core;

namespace Mackerel.RemoteCache.Client.Internal
{
    internal class AsyncStreamReaderCombiner<T> : IAsyncEnumerable<T>
    {
        private readonly IReadOnlyCollection<IAsyncStreamReader<T>> _collections;

        public AsyncStreamReaderCombiner(IReadOnlyCollection<IAsyncStreamReader<T>> collections)
        {
            _collections = collections;
        }

        public AsyncStreamReaderCombiner(params IAsyncStreamReader<T>[] collections)
        {
            _collections = collections;
        }

        public Enumerator GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new Enumerator(_collections, cancellationToken);
        }

        IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            return GetAsyncEnumerator(cancellationToken);
        }

        public class Enumerator : IAsyncEnumerator<T>
        {
            private readonly IReadOnlyCollection<IAsyncStreamReader<T>> _collections;
            private readonly CancellationToken _cancellationToken;

            private Channel<T> _channel;
            private List<Task> _tasks;
            private int _completed;
            private List<Exception> _exceptions;

            public T Current { get; private set; }

            public Enumerator(
                IReadOnlyCollection<IAsyncStreamReader<T>> collections,
                CancellationToken cancellationToken)
            {
                _collections = collections;
                _cancellationToken = cancellationToken;
            }

            public async ValueTask DisposeAsync()
            {
                if (_channel != null)
                    _channel.Writer.TryComplete();

                if (_tasks != null)
                {
                    try
                    {
                        await Task.WhenAll(_tasks);
                    }
                    catch { }
                }
            }

            public ValueTask<bool> MoveNextAsync()
            {
                if (_channel is null)
                {
                    StartConsuming();
                }

                if (_channel.Reader.TryRead(out var item))
                {
                    Current = item;
                    return new ValueTask<bool>(true);
                }

                return ReadAsync();
            }

            private void StartConsuming()
            {
                var options = new UnboundedChannelOptions
                {
                    SingleWriter = false,
                    SingleReader = true,
                    AllowSynchronousContinuations = false
                };

                _channel = Channel.CreateUnbounded<T>(options);
                _tasks = new List<Task>(_collections.Count);

                foreach (var collection in _collections)
                {
                    var task = RunLoop(collection);
                    _tasks.Add(task);
                }
            }

            private async Task RunLoop(IAsyncStreamReader<T> collection)
            {
                var writer = _channel.Writer;

                try
                {
                    while (await collection.MoveNext(_cancellationToken).ConfigureAwait(false))
                    {
                        await writer.WriteAsync(collection.Current, _cancellationToken);
                    }
                   
                    var completed = Interlocked.Increment(ref _completed);
                    if (completed == _collections.Count)
                    {
                        writer.TryComplete();
                    }
                }
                catch (Exception e)
                {
                    lock (_exceptions)
                    {
                        _exceptions ??= new List<Exception>();
                        _exceptions.Add(e);
                    }
                    writer.TryComplete();

                    throw e;
                }
            }

            private async ValueTask<bool> ReadAsync()
            {
                try
                {
                    var result = await _channel.Reader.ReadAsync(_cancellationToken);
                    Current = result;
                    return true;
                }
                catch (ChannelClosedException)
                {
                    return false;
                }
                catch (Exception e)
                {
                    if (_exceptions?.Count > 0)
                    {
                        if (_exceptions.Count == 1)
                        {
                            throw _exceptions[0];
                        }
                        else
                        {
                            new AggregateException(_exceptions);
                        }
                    }

                    throw e;
                }
            }
        }
    }
}
