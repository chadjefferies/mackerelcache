using System;
using System.Buffers.Binary;
using System.Threading;
using Google.Protobuf;

namespace Mackerel.RemoteCache.Server.Runtime
{
    /// <summary>
    /// The value used to store data in the cache
    /// </summary>
    public class CacheValue : IComparable<CacheValue>
    {
        public const int LONG_SIZE = sizeof(long);
        public const long TTL_NOT_FOUND = 2 * -TimeSpan.TicksPerMillisecond;

        private long _lastAccessed;

        public long AccessTime => Volatile.Read(ref _lastAccessed);
        public byte[] Value { get; }

        public CacheValue(byte[] value, long accessTime)
        {
            Value = value;
            _lastAccessed = accessTime;
        }

        public CacheValue(ByteString value, long accessTime)
        {
            Value = value?.Span.ToArray() ?? Array.Empty<byte>();
            _lastAccessed = accessTime;
        }

        public CacheValue(long value, long accessTime)
        {
            Value = new byte[LONG_SIZE];
            BinaryPrimitives.WriteInt64LittleEndian(Value, value);
            _lastAccessed = accessTime;
        }

        public bool IsExpired(long accessTime, long expirationTicks)
        {
            if (expirationTicks > 0 && accessTime != 0)
            {
                return AccessTime <= (accessTime - expirationTicks);
            }

            return false;
        }

        public void Touch(long accessTime)
        {
            Volatile.Write(ref _lastAccessed, accessTime);
        }

        public long Ttl(long accessTime, long expirationTicks)
        {
            if (expirationTicks > 0)
            {
                return expirationTicks - (accessTime - AccessTime);
            }

            return -TimeSpan.TicksPerMillisecond;
        }

        public bool TryIncrement(long value, out long result)
        {
            if (BinaryPrimitives.TryReadInt64LittleEndian(Value, out var x))
            {
                result = x + value;
                BinaryPrimitives.WriteInt64LittleEndian(Value, result);
                return true;
            }
            result = 0;
            return false;
        }

        public int CompareTo(CacheValue other)
        {
            if (AccessTime < other.AccessTime) return -1;
            if (AccessTime > other.AccessTime) return 1;
            return 0;
        }

        /// <summary>
        /// Converts a <see cref="CacheValue"/> to a <see cref="ByteString"/>.
        /// </summary>
        /// <param name="value">The <see cref="CacheValue"/> to convert.</param>
        public static implicit operator ByteString(CacheValue value)
        {
            if (value is null) return default;
            return ByteString.CopyFrom(value.Value, 0, value.Value.Length);
        }
    }
}
