using System;
using System.Buffers;

namespace Mackerel.RemoteCache.Server
{
    /// <summary>
    /// Represents a block of cache data. Designed to limit allocations and only live during the life of a request.
    /// </summary>
    public readonly ref struct CacheDataBlock<T>
    {
        private readonly T[] _data;

        public ReadOnlySpan<T> Data { get; }

        public ref T this[int index]
        {
            get
            {
                return ref _data[index];
            }
        }

        public CacheDataBlock(int count)
        {
            // array may be longer than count
            _data = ArrayPool<T>.Shared.Rent(count);
            Data = _data.AsSpan(0, count);
        }

        private CacheDataBlock(int start, int count, T[] arr)
        {
            _data = arr;
            Data = arr.AsSpan(start, count);
        }

        public CacheDataBlock<T> Slice(int start, int count)
        {
            return new CacheDataBlock<T>(start, count, _data);
        }

        public void Dispose()
        {
            if (_data != null)
                ArrayPool<T>.Shared.Return(_data, true);
        }

        public static CacheDataBlock<T> Empty() => new CacheDataBlock<T>(0, 0, Array.Empty<T>());
    }
}
