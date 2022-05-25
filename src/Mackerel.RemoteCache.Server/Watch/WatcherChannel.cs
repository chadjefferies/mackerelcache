using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using Mackerel.RemoteCache.Server.Runtime;

namespace Mackerel.RemoteCache.Server.Watch
{
    /// <summary>
    /// A channel for producing watch events to a single watch stream.
    /// </summary>
    public class WatcherChannel : IEquatable<WatcherChannel>, IDisposable
    {
        private static readonly UnboundedChannelOptions _channelOptions = new UnboundedChannelOptions
        {
            AllowSynchronousContinuations = false,
            SingleReader = true,
            SingleWriter = false,
        };

        private readonly Channel<ChangeEvent> _channel;
        private readonly ChannelWriter<ChangeEvent> _writer;
        private readonly ChannelReader<ChangeEvent> _reader;
        private int _predicates; // TODO: move to stats pattern

        public CacheKey Id { get; }
        public int Predicates => _predicates;

        public WatcherChannel(CacheKey id)
        {
            Id = id;
            _channel = Channel.CreateUnbounded<ChangeEvent>(_channelOptions);
            _writer = _channel.Writer;
            _reader = _channel.Reader;
        }

        public bool TryWrite(ChangeEvent message) =>
            _writer.TryWrite(message);

        public IAsyncEnumerable<ChangeEvent> ReadAsync(CancellationToken token) =>
            _reader.ReadAllAsync(token);

        public void Dispose() => 
            _writer.TryComplete();

        public void IncrementPredicates()
        {
            Interlocked.Increment(ref _predicates);
        }

        public void DecrementPredicates()
        {
            Interlocked.Decrement(ref _predicates);
        }

        public bool Equals(WatcherChannel other)
        {
            return other.Id == Id;
        }

        public override bool Equals(object obj)
        {
            if (obj is WatcherChannel k) return Equals(k);
            return Equals((WatcherChannel)obj);
        }

        public override int GetHashCode() => Id.GetHashCode();

        public static bool operator ==(WatcherChannel x, WatcherChannel y) => x.Equals(y);

        public static bool operator !=(WatcherChannel x, WatcherChannel y) => !(x == y);
    }
}
