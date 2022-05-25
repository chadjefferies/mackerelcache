using System;
using System.Buffers;
using System.Collections.Generic;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Util;

namespace Mackerel.RemoteCache.Server.Eviction
{
    public class RandomLruEvictionPolicy : IEvictionPolicy
    {
        private int[] _randomIndexes = Array.Empty<int>();
        private KeyValuePair<CacheKey, CacheValue>[] _itemsToEvict = Array.Empty<KeyValuePair<CacheKey, CacheValue>>();

        private void InitArrayPools(int size)
        {
            if (size > _randomIndexes.Length)
            {
                ArrayPool<int>.Shared.Return(_randomIndexes);
                _randomIndexes = ArrayPool<int>.Shared.Rent(size);
            }

            if (size > _itemsToEvict.Length)
            {
                ArrayPool<KeyValuePair<CacheKey, CacheValue>>.Shared.Return(_itemsToEvict, true);
                _itemsToEvict = ArrayPool<KeyValuePair<CacheKey, CacheValue>>.Shared.Rent(size);
            }
        }

        public ReadOnlySpan<KeyValuePair<CacheKey, CacheValue>> GetItems(MemoryStorePartition partition, int count, DateTime accessTime)
        {
            int sampleSize = partition.Conf.KeyEvictionSamples;
            int relativeSampleSize = partition.Conf.KeyEvictionSamples * count;

            Span<KeyValuePair<CacheKey, CacheValue>> spItemsToEvict;

            InitArrayPools(relativeSampleSize);

            if (relativeSampleSize >= partition.Stats.CurrentItemCount)
            {
                int i = 0;
                foreach (var item in partition)
                {
                    _itemsToEvict[i] = item;
                    i++;
                }

                spItemsToEvict = _itemsToEvict.AsSpan(0, partition.Stats.CurrentItemCount);
            }
            else
            {
                var threadLocalRandom = ThreadLocalRandom.Current;
                for (int i = 0; i < relativeSampleSize; i++)
                {
                    _randomIndexes[i] = threadLocalRandom.Next(partition.Stats.CurrentItemCount);
                }

                var spRandomIndexes = _randomIndexes.AsSpan(0, relativeSampleSize);

                spRandomIndexes.Sort();

                int index = 0, indexRandom = 0, itemsIndex = 0;

                var enumerator = partition.GetEnumerator();
                while (enumerator.MoveNext() && indexRandom < relativeSampleSize && itemsIndex < relativeSampleSize)
                {
                    var current = enumerator.Current;
                    if (spRandomIndexes[indexRandom] < index)
                    {
                        // handle dupes in random indexes
                        indexRandom++;
                        continue;
                    }

                    if (current.Value.IsExpired(accessTime.Ticks, partition.Metadata.ExpirationTicks))
                    {
                        _itemsToEvict[itemsIndex] = current;
                        itemsIndex++;
                    }
                    else if (spRandomIndexes[indexRandom] == index)
                    {
                        _itemsToEvict[itemsIndex] = current;
                        indexRandom++;
                        itemsIndex++;
                    }

                    index++;
                }

                spItemsToEvict = _itemsToEvict.AsSpan(0, itemsIndex);
            }

            spItemsToEvict.Sort((x, y) => x.Value.CompareTo(y.Value));

            //if (count < spItemsToEvict.Length)
            //{
            //    spItemsToEvict = spItemsToEvict.Slice(0, count);
            //}

            return spItemsToEvict;
        }

        public void Dispose()
        {
            ArrayPool<int>.Shared.Return(_randomIndexes);
            ArrayPool<KeyValuePair<CacheKey, CacheValue>>.Shared.Return(_itemsToEvict, true);
        }
    }
}
