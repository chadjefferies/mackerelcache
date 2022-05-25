using System;
using System.Buffers.Binary;
using System.Text;
using Mackerel.RemoteCache.Server.Util;

namespace Mackerel.RemoteCache.Server.Runtime
{
    /// <summary>
    /// The key used to store data and perform lookups in the cache
    /// </summary>
    public readonly struct CacheKey : IEquatable<CacheKey>, IComparable<CacheKey>
    {
        public byte[] Key { get; }

        public CacheKey(byte[] key)
        {
            Key = key ?? Array.Empty<byte>();
        }

        public CacheKey(long key)
        {
            Key = new byte[CacheValue.LONG_SIZE];
            BinaryPrimitives.WriteInt64LittleEndian(Key, key);
        }

        #region IEquatable

        public bool Equals(CacheKey other)
        {
            return Key.AsSpan().SequenceEqual(other.Key);
        }

        public override bool Equals(object obj)
        {
            if (obj is CacheKey k) return Equals(k);
            return Equals((CacheKey)obj);
        }

        public override int GetHashCode() => (int)MurmurHash3.Hash128(Key);

        public override string ToString()
        {
            return this;
        }

        /// <summary>
        /// Creates a new <see cref="CacheKey"/> from an <see cref="T:byte[]"/>.
        /// </summary>
        /// <param name="value">The <see cref="T:byte[]"/> to convert to a <see cref="CacheKey"/>.</param>
        public static implicit operator CacheKey(byte[] value)
            => new CacheKey(value);

        /// <summary>
        /// Converts a <see cref="CacheKey"/> to a <see cref="T:byte[]"/>.
        /// </summary>
        /// <param name="value">The <see cref="CacheKey"/> to convert.</param>
        public static implicit operator byte[] (CacheKey value)
            => value.Key;

        /// <summary>
        /// Creates a new <see cref="CacheKey"/> from an <see cref="string"/>.
        /// </summary>
        /// <param name="value">The <see cref="string"/> to convert to a <see cref="CacheKey"/>.</param>
        /// <remarks>
        /// If https://github.com/dotnet/runtime/issues/27229 ever becomes
        /// a thing, we can change how this works for key lookups 
        /// and reduce allocations significantly.
        /// </remarks>
        public static implicit operator CacheKey(string value)
        {
            if (value == null)
                return default;

            var key = Encoding.UTF8.GetBytes(value);
            return new CacheKey(key);
        }

        /// <summary>
        /// Converts a <see cref="CacheKey"/> to a <see cref="string"/>.
        /// </summary>
        /// <param name="value">The <see cref="CacheKey"/> to convert.</param>
        public static implicit operator string(CacheKey value)
        {
            return Encoding.UTF8.GetString(value.Key.AsSpan());
        }

        public static bool operator ==(CacheKey x, CacheKey y) => x.Equals(y);

        public static bool operator !=(CacheKey x, CacheKey y) => !(x == y);

        public static bool operator ==(CacheKey x, byte[] y) => x.Equals(y);

        public static bool operator !=(CacheKey x, byte[] y) => !(x == y);

        public static bool operator ==(byte[] x, CacheKey y) => y.Equals(x);

        public static bool operator !=(byte[] x, CacheKey y) => !(y == x);


        public static bool operator <(CacheKey x, CacheKey y) =>
            x.Key.AsSpan().SequenceCompareTo(y.Key.AsSpan()) < 0;

        public static bool operator <=(CacheKey x, CacheKey y) =>
            x.Key.AsSpan().SequenceCompareTo(y.Key.AsSpan()) <= 0;

        public static bool operator >(CacheKey x, CacheKey y) => y < x;

        public static bool operator >=(CacheKey x, CacheKey y) => y <= x;

        #endregion

        #region IComparable

        public int CompareTo(CacheKey other)
        {
            return Key.AsSpan().SequenceCompareTo(other.Key.AsSpan());
        }

        #endregion
    }
}
