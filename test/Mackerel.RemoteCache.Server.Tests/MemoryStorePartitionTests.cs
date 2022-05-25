using System;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Eviction;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Statistics;
using Mackerel.RemoteCache.Server.Tests.Util;
using Microsoft.Extensions.Internal;
using Moq;
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests
{
    public class MemoryStorePartitionTests
    {
        private readonly Mock<ISystemClock> _mockClock;

        public MemoryStorePartitionTests()
        {
            _mockClock = new Mock<ISystemClock>();
        }

        [Fact]
        public void Default()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,
            };

            using (var cache = GetPartition(conf))
            {
                Assert.Equal(conf, cache.Conf);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void Put()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,
            };

            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.Put(key, value, default);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);

                var valueFromCache = cache.Get(key, default, true, true);
                Assert.Equal(value, valueFromCache);
            }
        }

        [Fact]
        public void Put_NullKey()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,
            };


            using (var cache = GetPartition(conf))
            {
                var value = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.Put(default, value, default);

                Assert.Equal(WriteResult.MissingKey, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);

                Assert.Null(cache.Get(default(CacheKey), default, true, true));
            }
        }

        [Fact]
        public void Put_NullValue()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,
            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var putStatus = cache.Put(key, default, default);

                Assert.Equal(WriteResult.MissingValue, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);

                Assert.Null(cache.Get(key, default, true, true));
            }
        }

        [Fact]
        public void Put_KeyTooLarge()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 1024,
            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(Helpers.BuildRandomByteArray(1500));
                var value = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.Put(key, value, default);

                Assert.Equal(WriteResult.KeyTooLarge, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);

                Assert.Null(cache.Get(key, default, true, true));
            }
        }

        [Fact]
        public void Put_ItemTooLarge()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 1024,
                MaxBytesPerKey = 2048,
            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(Helpers.BuildRandomByteArray(1500));
                var putStatus = cache.Put(key, value, default);

                Assert.Equal(WriteResult.ValueTooLarge, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);

                Assert.Null(cache.Get(key, default, true, true));
            }
        }

        [Fact]
        public void Put_Update()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,
            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value1, default);

                var value2 = ByteString.CopyFrom(new byte[] { 33 });
                var putStatus = cache.Put(key, value2, default);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);

                var valueFromCache = cache.Get(key, default, true, true);
                Assert.Equal(value2, valueFromCache);
            }
        }

        [Fact]
        public void Put_Update_ItemNotChanged()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value, default);

                var putStatus = cache.Put(key, value, default);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);

                var valueFromCache = cache.Get(key, default, true, true);
                Assert.Equal(value, valueFromCache);
            }
        }

        [Fact]
        public void Put_Update_ItemNotChanged_DeepCopy()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var key1 = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key1, value1, default);

                var key2 = new CacheKey(new byte[] { 11 });
                var value2 = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.Put(key2, value2, default);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);

                var valueFromCache = cache.Get(key2, default, true, true);
                Assert.Equal(value2, valueFromCache);
            }
        }

        [Fact]
        public void Put_Update_Expired_Sliding()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, false);
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value1, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var value2 = ByteString.CopyFrom(new byte[] { 33 });
                var putStatus = cache.Put(key, value2, secondPutTime);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(2, cache.Stats.TotalCacheSize);

                var getTime = secondPutTime.AddTicks(1);
                var valueFromCache = cache.Get(key, getTime, true, true);
                Assert.Equal(value2, valueFromCache);
            }
        }

        [Fact]
        public void Put_Update_Expired_Absolute()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, true);
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value1, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var value2 = ByteString.CopyFrom(new byte[] { 33 });
                var putStatus = cache.Put(key, value2, secondPutTime);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(2, cache.Stats.TotalCacheSize);

                var getTime = secondPutTime.AddTicks(1);
                var valueFromCache = cache.Get(key, getTime, true, true);
                Assert.Equal(value2, valueFromCache);
            }
        }

        [Fact]
        public void Put_InsufficientCapacity()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                cache.Metadata.SetSize(2);
                cache.Metadata.SetEvictionPolicy(EvictionPolicy.NoEviction);
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.Put(key, value1, DateTime.Parse("3:00:00 PM"));
                Assert.Equal(WriteResult.Success, putStatus);

                var key2 = new CacheKey(new byte[] { 33 });
                var value2 = ByteString.CopyFrom(new byte[] { 44 });
                putStatus = cache.Put(key2, value2, DateTime.Parse("3:00:00 PM"));

                Assert.Equal(WriteResult.InsufficientCapacity, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(2, cache.Stats.TotalCacheSize);
            }
        }

        [Fact]
        public void PutMany()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.Put(items, default);

                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);

                using var dataBlock = cache.Get(items.Keys.ToList(), default, true, true);
                Assert.Equal(items.Count, dataBlock.Data.Length);
                Assert.Equal(items.Count, dataBlock.Data.Length);
                Assert.Equal(items.First().Key, dataBlock.Data[0].Key);
                Assert.Equal(items.First().Value, dataBlock.Data[0].Value);
                Assert.Equal(items.Last().Key, dataBlock.Data[1].Key);
                Assert.Equal(items.Last().Value, dataBlock.Data[1].Value);

            }
        }

        [Fact]
        public void PutMany_SingleNullKey_Atomic()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey((byte[])null), ByteString.CopyFrom(new byte[] { 2 }) }
                };
                Assert.Equal(WriteResult.MissingKey, cache.Put(items, default));

                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);

                using var dataBlock = cache.Get(items.Keys.ToList(), default, true, true);
                Assert.Equal(0, dataBlock.Data.Length);
            }
        }

        [Fact]
        public void PutMany_Update_Expired_Sliding()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, false);

                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.Put(items, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var putStatus = cache.Put(items, secondPutTime);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(2, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);
                Assert.Equal(4, cache.Stats.TotalCacheSize);

                var getTime = secondPutTime.AddTicks(1);
                using var values = cache.Get(items.Keys.ToList(), getTime, true, true);
                Assert.Equal(2, values.Data.Length);
                Assert.Equal(items.First().Key, values.Data[0].Key);
                Assert.Equal(items.First().Value, values.Data[0].Value);
                Assert.Equal(items.Last().Key, values.Data[1].Key);
                Assert.Equal(items.Last().Value, values.Data[1].Value);
            }
        }

        [Fact]
        public void PutMany_Update_Expired_Absolute()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, true);

                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.Put(items, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var putStatus = cache.Put(items, secondPutTime);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(2, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);
                Assert.Equal(4, cache.Stats.TotalCacheSize);

                var getTime = secondPutTime.AddTicks(1);
                using var values = cache.Get(items.Keys.ToList(), getTime, true, true);
                Assert.Equal(2, values.Data.Length);
                Assert.Equal(items.First().Key, values.Data[0].Key);
                Assert.Equal(items.First().Value, values.Data[0].Value);
                Assert.Equal(items.Last().Key, values.Data[1].Key);
                Assert.Equal(items.Last().Value, values.Data[1].Value);
            }
        }

        [Fact]
        public void PutMany_InsufficientCapacity()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                cache.Metadata.SetSize(4);
                cache.Metadata.SetEvictionPolicy(EvictionPolicy.NoEviction);

                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                var putStatus = cache.Put(items, DateTime.Parse("3:00:00 PM"));
                Assert.Equal(WriteResult.Success, putStatus);

                var items2 = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 33 }), ByteString.CopyFrom(new byte[] { 3 }) },
                    { new CacheKey(new byte[] { 44 }), ByteString.CopyFrom(new byte[] { 4 }) }
                };
                putStatus = cache.Put(items2, DateTime.Parse("3:00:00 PM"));

                Assert.Equal(WriteResult.InsufficientCapacity, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);
                Assert.Equal(4, cache.Stats.TotalCacheSize);
            }
        }


        [Fact]
        public void PutIfNotExists()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.PutIfNotExists(key, value, default);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void PutIfNotExists_NullKey()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var value = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.PutIfNotExists(default, value, default);

                Assert.Equal(WriteResult.MissingKey, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void PutIfNotExists_NullValue()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var putStatus = cache.PutIfNotExists(key, default, default);

                Assert.Equal(WriteResult.MissingValue, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void PutIfNotExists_KeyTooLarge()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 1024,

            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(Helpers.BuildRandomByteArray(1500));
                var value = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.PutIfNotExists(key, value, default);

                Assert.Equal(WriteResult.KeyTooLarge, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void PutIfNotExists_ItemTooLarge()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 1024,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(Helpers.BuildRandomByteArray(1500));
                var putStatus = cache.PutIfNotExists(key, value, default);

                Assert.Equal(WriteResult.ValueTooLarge, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void PutIfNotExists_Update()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value1, default);

                var value2 = ByteString.CopyFrom(new byte[] { 33 });
                var putStatus = cache.PutIfNotExists(key, value2, default);

                Assert.Equal(WriteResult.KeyAlreadyExists, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);

                var valueFromCache = cache.Get(key, default, true, true);
                Assert.Equal(value1, valueFromCache);
            }
        }

        [Fact]
        public void PutIfNotExists_Update_ItemNotChanged()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value, default);

                var putStatus = cache.PutIfNotExists(key, value, default);

                Assert.Equal(WriteResult.KeyAlreadyExists, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);

                var valueFromCache = cache.Get(key, default, true, true);
                Assert.Equal(value, valueFromCache);
            }
        }

        [Fact]
        public void PutIfNotExistst_Update_ItemNotChanged_DeepCopy()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var key1 = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.PutIfNotExists(key1, value1, default);

                var key2 = new CacheKey(new byte[] { 11 });
                var value2 = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.PutIfNotExists(key2, value2, default);

                Assert.Equal(WriteResult.KeyAlreadyExists, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);

                var valueFromCache = cache.Get(key1, default, true, true);
                Assert.Equal(value1, valueFromCache);
            }
        }

        [Fact]
        public void PutIfNotExists_Update_Expired_Sliding()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, false);
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.PutIfNotExists(key, value1, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var value2 = ByteString.CopyFrom(new byte[] { 33 });
                var putStatus = cache.PutIfNotExists(key, value2, secondPutTime);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(2, cache.Stats.TotalCacheSize);

                var getTime = secondPutTime.AddTicks(1);
                var valueFromCache = cache.Get(key, getTime, true, true);
                Assert.Equal(value2, valueFromCache);
            }
        }

        [Fact]
        public void PutIfNotExists_Update_Expired_Absolute()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, true);
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.PutIfNotExists(key, value1, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var value2 = ByteString.CopyFrom(new byte[] { 33 });
                var putStatus = cache.PutIfNotExists(key, value2, secondPutTime);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(2, cache.Stats.TotalCacheSize);

                var getTime = secondPutTime.AddTicks(1);
                var valueFromCache = cache.Get(key, getTime, true, true);
                Assert.Equal(value2, valueFromCache);
            }
        }

        [Fact]
        public void PutIfNotExists_InsufficientCapacity()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                cache.Metadata.SetSize(2);
                cache.Metadata.SetEvictionPolicy(EvictionPolicy.NoEviction);
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.PutIfNotExists(key, value1, DateTime.Parse("3:00:00 PM"));
                Assert.Equal(WriteResult.Success, putStatus);

                var key2 = new CacheKey(new byte[] { 33 });
                var value2 = ByteString.CopyFrom(new byte[] { 44 });
                putStatus = cache.PutIfNotExists(key2, value2, DateTime.Parse("3:00:00 PM"));

                Assert.Equal(WriteResult.InsufficientCapacity, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(2, cache.Stats.TotalCacheSize);
            }
        }

        [Fact]
        public void PutIfNotExistsMany()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.PutIfNotExists(items, default);

                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);

                using var dataBlock = cache.Get(items.Keys.ToList(), default, true, true);
                Assert.Equal(items.Count, dataBlock.Data.Length);
                Assert.Equal(items.First().Key, dataBlock.Data[0].Key);
                Assert.Equal(items.First().Value, dataBlock.Data[0].Value);
                Assert.Equal(items.Last().Key, dataBlock.Data[1].Key);
                Assert.Equal(items.Last().Value, dataBlock.Data[1].Value);
            }
        }

        [Fact]
        public void PutIfNotExistsMany_TryUpdate()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var items1 = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.Put(items1, default);

                var items2 = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 11 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 22 }) }
                };

                cache.PutIfNotExists(items2, default);

                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);

                using var dataBlock = cache.Get(items1.Keys.ToList(), default, true, true);
                Assert.Equal(items1.Count, dataBlock.Data.Length);
                Assert.Equal(items1.First().Key, dataBlock.Data[0].Key);
                Assert.Equal(items1.First().Value, dataBlock.Data[0].Value);
                Assert.Equal(items1.Last().Key, dataBlock.Data[1].Key);
                Assert.Equal(items1.Last().Value, dataBlock.Data[1].Value);
            }
        }

        [Fact]
        public void PutIfNotExistsMany_SingleNullKey_Atomic()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey((byte[])null), ByteString.CopyFrom(new byte[] { 2 }) }
                };
                Assert.Equal(WriteResult.MissingKey, cache.PutIfNotExists(items, default));

                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);

                using var dataBlock = cache.Get(items.Keys.ToList(), default, true, true);
                Assert.Equal(0, dataBlock.Data.Length);
            }
        }

        [Fact]
        public void PutIfNotExistsMany_Update_Expired_Sliding()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, false);

                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.PutIfNotExists(items, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var putStatus = cache.PutIfNotExists(items, secondPutTime);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(2, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);
                Assert.Equal(4, cache.Stats.TotalCacheSize);

                var getTime = secondPutTime.AddTicks(1);
                using var values = cache.Get(items.Keys.ToList(), getTime, true, true);
                Assert.Equal(2, values.Data.Length);
                Assert.Equal(items.First().Key, values.Data[0].Key);
                Assert.Equal(items.First().Value, values.Data[0].Value);
                Assert.Equal(items.Last().Key, values.Data[1].Key);
                Assert.Equal(items.Last().Value, values.Data[1].Value);
            }
        }

        [Fact]
        public void PutIfNotExistsMany_Update_Expired_Absolute()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, true);

                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.PutIfNotExists(items, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var putStatus = cache.PutIfNotExists(items, secondPutTime);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(2, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);
                Assert.Equal(4, cache.Stats.TotalCacheSize);

                var getTime = secondPutTime.AddTicks(1);
                using var values = cache.Get(items.Keys.ToList(), getTime, true, true);
                Assert.Equal(2, values.Data.Length);
                Assert.Equal(items.First().Key, values.Data[0].Key);
                Assert.Equal(items.First().Value, values.Data[0].Value);
                Assert.Equal(items.Last().Key, values.Data[1].Key);
                Assert.Equal(items.Last().Value, values.Data[1].Value);
            }
        }

        [Fact]
        public void PutIfNotExistsMany_InsufficientCapacity()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                cache.Metadata.SetSize(4);
                cache.Metadata.SetEvictionPolicy(EvictionPolicy.NoEviction);

                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                var putStatus = cache.PutIfNotExists(items, DateTime.Parse("3:00:00 PM"));
                Assert.Equal(WriteResult.Success, putStatus);

                var items2 = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 33 }), ByteString.CopyFrom(new byte[] { 3 }) },
                    { new CacheKey(new byte[] { 44 }), ByteString.CopyFrom(new byte[] { 4 }) }
                };
                putStatus = cache.PutIfNotExists(items2, DateTime.Parse("3:00:00 PM"));

                Assert.Equal(WriteResult.InsufficientCapacity, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);
                Assert.Equal(4, cache.Stats.TotalCacheSize);
            }
        }


        [Fact]
        public void PutIfExists()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.PutIfExists(key, value, default);

                Assert.Equal(WriteResult.KeyDoesNotExist, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void PutIfExists_NullKey()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var value = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.PutIfExists(default, value, default);

                Assert.Equal(WriteResult.MissingKey, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void PutIfExists_NullValue()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var putStatus = cache.PutIfExists(key, default, default);

                Assert.Equal(WriteResult.MissingValue, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void PutIfExists_KeyTooLarge()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 1024,

            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(Helpers.BuildRandomByteArray(1500));
                var value = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.PutIfExists(key, value, default);

                Assert.Equal(WriteResult.KeyTooLarge, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void PutIfExists_ItemTooLarge()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 1024,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(Helpers.BuildRandomByteArray(1500));
                var putStatus = cache.PutIfExists(key, value, default);

                Assert.Equal(WriteResult.ValueTooLarge, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void PutIfExists_Update()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value1, default);

                var value2 = ByteString.CopyFrom(new byte[] { 33 });
                var putStatus = cache.PutIfExists(key, value2, default);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);

                var valueFromCache = cache.Get(key, default, true, true);
                Assert.Equal(value2, valueFromCache);
            }
        }

        [Fact]
        public void PutIfExists_Update_ItemNotChanged()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value, default);

                var putStatus = cache.PutIfExists(key, value, default);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);

                var valueFromCache = cache.Get(key, default, true, true);
                Assert.Equal(value, valueFromCache);
            }
        }

        [Fact]
        public void PutIfExists_Update_ItemNotChanged_DeepCopy()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var key1 = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key1, value1, default);

                var key2 = new CacheKey(new byte[] { 11 });
                var value2 = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.PutIfExists(key2, value2, default);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);

                var valueFromCache = cache.Get(key2, default, true, true);
                Assert.Equal(value2, valueFromCache);
            }
        }

        [Fact]
        public void PutIfExists_Update_Expired_Sliding()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, false);
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value1, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var value2 = ByteString.CopyFrom(new byte[] { 33 });
                var putStatus = cache.PutIfExists(key, value2, secondPutTime);

                Assert.Equal(WriteResult.KeyDoesNotExist, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
                Assert.Equal(0, cache.Stats.TotalCacheSize);

                var getTime = secondPutTime.AddTicks(1);
                var valueFromCache = cache.Get(key, getTime, true, true);
                Assert.Null(valueFromCache);
            }
        }

        [Fact]
        public void PutIfExists_Update_Expired_Absolute()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, true);
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value1, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var value2 = ByteString.CopyFrom(new byte[] { 33 });
                var putStatus = cache.PutIfExists(key, value2, secondPutTime);

                Assert.Equal(WriteResult.KeyDoesNotExist, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
                Assert.Equal(0, cache.Stats.TotalCacheSize);

                var getTime = secondPutTime.AddTicks(1);
                var valueFromCache = cache.Get(key, getTime, true, true);
                Assert.Null(valueFromCache);
            }
        }

        [Fact]
        public void PutIfExists_InsufficientCapacity()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                cache.Metadata.SetSize(2);
                cache.Metadata.SetEvictionPolicy(EvictionPolicy.NoEviction);
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                var putStatus = cache.Put(key, value1, DateTime.Parse("3:00:00 PM"));
                Assert.Equal(WriteResult.Success, putStatus);

                var value2 = ByteString.CopyFrom(Guid.NewGuid().ToByteArray());
                putStatus = cache.PutIfExists(key, value2, DateTime.Parse("3:00:00 PM"));

                Assert.Equal(WriteResult.InsufficientCapacity, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(2, cache.Stats.TotalCacheSize);

                var valueFromCache = cache.Get(key, DateTime.Parse("3:00:00 PM"), true, true);
                Assert.Equal(value1, valueFromCache);
            }
        }

        [Fact]
        public void PutIfExistsMany_TryInsert()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };
                cache.PutIfExists(items, default);

                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);

                using var dataBlock = cache.Get(items.Keys.ToList(), default, true, true);
                Assert.Empty(dataBlock.Data.ToArray());

                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(2, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void PutIfExistsMany_SingleNullKey_Atomic()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var items1 = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.Put(items1, default);

                var items2 = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 11 }) },
                    { new CacheKey(((byte[])null)), ByteString.CopyFrom(new byte[] { 22 }) }
                };

                Assert.Equal(WriteResult.MissingKey, cache.PutIfExists(items2, default));

                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);

                using var dataBlock = cache.Get(items1.Keys.ToList(), default, true, true);
                Assert.Equal(items1.Count, dataBlock.Data.Length);
                Assert.Equal(items1.First().Key, dataBlock.Data[0].Key);
                Assert.Equal(items1.First().Value, dataBlock.Data[0].Value);
                Assert.Equal(items1.Last().Key, dataBlock.Data[1].Key);
                Assert.Equal(items1.Last().Value, dataBlock.Data[1].Value);
            }
        }

        [Fact]
        public void PutIfExistsMany_Update()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var items1 = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.Put(items1, default);

                var items2 = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 11 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 22 }) }
                };

                cache.PutIfExists(items2, default);

                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);

                using var dataBlock = cache.Get(items1.Keys.ToList(), default, true, true);
                Assert.Equal(items1.Count, dataBlock.Data.Length);
                Assert.Equal(items2.First().Key, dataBlock.Data[0].Key);
                Assert.Equal(items2.First().Value, dataBlock.Data[0].Value);
                Assert.Equal(items2.Last().Key, dataBlock.Data[1].Key);
                Assert.Equal(items2.Last().Value, dataBlock.Data[1].Value);
            }
        }

        [Fact]
        public void PutIfExistsMany_Update_Expired_Sliding()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, false);

                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.Put(items, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var putStatus = cache.PutIfExists(items, secondPutTime);

                Assert.Equal(WriteResult.KeyDoesNotExist, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount); // lazy expiration, once it encounters the first "failure" in the batch, it stops.
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(2, cache.Stats.TotalCacheSize);

                var getTime = secondPutTime.AddTicks(1);
                using var values = cache.Get(items.Keys.ToList(), getTime, true, true);
                Assert.Equal(0, values.Data.Length);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(2, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(2, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
                Assert.Equal(0, cache.Stats.TotalCacheSize);
            }
        }

        [Fact]
        public void PutIfExistsMany_Update_Expired_Absolute()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, true);

                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.Put(items, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var putStatus = cache.PutIfExists(items, secondPutTime);

                Assert.Equal(WriteResult.KeyDoesNotExist, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount); // lazy expiration, once it encounters the first "failure" in the batch, it stops.
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(2, cache.Stats.TotalCacheSize);

                var getTime = secondPutTime.AddTicks(1);
                using var values = cache.Get(items.Keys.ToList(), getTime, true, true);
                Assert.Equal(0, values.Data.Length);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(2, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(2, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
                Assert.Equal(0, cache.Stats.TotalCacheSize);
            }
        }

        [Fact]
        public void PutIfExistsMany_InsufficientCapacity()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                cache.Metadata.SetSize(4);
                cache.Metadata.SetEvictionPolicy(EvictionPolicy.NoEviction);

                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                var putStatus = cache.Put(items, DateTime.Parse("3:00:00 PM"));
                Assert.Equal(WriteResult.Success, putStatus);

                var items2 = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(Guid.NewGuid().ToByteArray()) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(Guid.NewGuid().ToByteArray()) }
                };
                putStatus = cache.PutIfExists(items2, DateTime.Parse("3:00:00 PM"));

                Assert.Equal(WriteResult.InsufficientCapacity, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);
                Assert.Equal(4, cache.Stats.TotalCacheSize);
            }
        }


        [Fact]
        public void Get()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value, default);

                var getValue = cache.Get(key, default, true, true);

                Assert.Equal(value, getValue);
                Assert.Equal(1, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void Get_NullKey()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value, default);

                var getValue = cache.Get(default(CacheKey), default, true, true);

                Assert.Null(getValue);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(1, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void Get_MissingKey()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var key1 = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key1, value, default);

                var key2 = new CacheKey(new byte[] { 33 });
                var getValue = cache.Get(key2, default, true, true);

                Assert.Null(getValue);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(1, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void Get_Sliding_Expired()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                cache.Metadata.SetExpiration(TimeSpan.FromHours(1).Ticks, false);
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value, DateTime.Parse("2015-09-21 08:00:00"));

                var getValue = cache.Get(key, DateTime.Parse("2015-09-21 08:59:00"), true, true);
                Assert.NotNull(getValue);

                var getValue1 = cache.Get(key, DateTime.Parse("2015-09-21 09:01:00"), true, true);
                Assert.NotNull(getValue);

                var getValue2 = cache.Get(key, DateTime.Parse("2015-09-21 10:02:00"), true, true);

                Assert.Null(getValue2);
                Assert.Equal(2, cache.Stats.TotalHits);
                Assert.Equal(1, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void Get_Absolute_Expired()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                cache.Metadata.SetExpiration(TimeSpan.FromHours(1).Ticks, true);
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value, DateTime.Parse("2015-09-21 08:00:00"));

                var getValue = cache.Get(key, DateTime.Parse("2015-09-21 08:59:00"), true, true);
                Assert.NotNull(getValue);

                var getValue2 = cache.Get(key, DateTime.Parse("2015-09-21 09:01:00"), true, true);

                Assert.Null(getValue2);
                Assert.Equal(1, cache.Stats.TotalHits);
                Assert.Equal(1, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void GetPut_Absolute_Expired()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                cache.Metadata.SetExpiration(TimeSpan.FromHours(1).Ticks, true);
                var key = "11";
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value, DateTime.Parse("2015-09-21 08:00:00"));

                // should not "touch" the entry
                var putResult = cache.Put(key, value, DateTime.Parse("2015-09-21 08:59:00"));
                Assert.Equal(WriteResult.Success, putResult);

                var getValue2 = cache.Get(key, DateTime.Parse("2015-09-21 09:01:00"), true, true);

                Assert.Null(getValue2);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(1, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void GetPutIfExists_Absolute_Expired()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                cache.Metadata.SetExpiration(TimeSpan.FromHours(1).Ticks, true);
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value, DateTime.Parse("2015-09-21 08:00:00"));

                // should not "touch" the entry
                var putResult = cache.PutIfExists(key, value, DateTime.Parse("2015-09-21 08:59:00"));
                Assert.Equal(WriteResult.Success, putResult);

                var getValue2 = cache.Get(key, DateTime.Parse("2015-09-21 09:01:00"), true, true);

                Assert.Null(getValue2);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(1, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void GetPutIfExists_Sliding_Expired()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                cache.Metadata.SetExpiration(TimeSpan.FromHours(1).Ticks, false);
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value, DateTime.Parse("2015-09-21 08:00:00"));

                // should "touch" the entry
                var putResult = cache.PutIfExists(key, value, DateTime.Parse("2015-09-21 08:59:00"));
                Assert.Equal(WriteResult.Success, putResult);

                var getValue2 = cache.Get(key, DateTime.Parse("2015-09-21 09:01:00"), true, true);

                Assert.Equal(value, getValue2);
                Assert.Equal(1, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void GetMany()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };


            using (var cache = GetPartition(conf))
            {
                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.Put(items, default);

                using var dataBlock = cache.Get(items.Keys.ToList(), default, true, true);
                Assert.Equal(items.Count, dataBlock.Data.Length);
                Assert.Equal(items.First().Key, dataBlock.Data[0].Key);
                Assert.Equal(items.First().Value, dataBlock.Data[0].Value);
                Assert.Equal(items.Last().Key, dataBlock.Data[1].Key);
                Assert.Equal(items.Last().Value, dataBlock.Data[1].Value);

                Assert.Equal(2, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void GetMany_Sliding_Expired()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                cache.Metadata.SetExpiration(TimeSpan.FromHours(1).Ticks, false);
                var items = new Dictionary<string, ByteString>
                {
                    { "11", ByteString.CopyFrom(new byte[] { 1 }) },
                    { "22", ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.Put(items, DateTime.Parse("2015-09-21 08:00:00"));
                // touch the first key a minute later, sliding it forward.
                var result = cache.Get(items.First().Key, DateTime.Parse("2015-09-21 08:02:00"), true, true);
                Assert.Equal(items.First().Value, result);


                using var dataBlock = cache.Get(items.Keys.ToList(), DateTime.Parse("2015-09-21 09:01:00"), true, true);
                // request both keys, only the one we didn't touch should be expired
                Assert.Equal(1, dataBlock.Data.Length);

                var rArr = dataBlock.Data.ToArray();
                Assert.Single(rArr);
                Assert.Equal(items.First().Key, rArr[0].Key);
                Assert.Equal(items.First().Value, rArr[0].Value);

                Assert.Equal(2, cache.Stats.TotalHits);
                Assert.Equal(1, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void GetMany_Absolute_Expired()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                cache.Metadata.SetExpiration(TimeSpan.FromHours(1).Ticks, true);
                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                cache.Put(items, DateTime.Parse("2015-09-21 08:00:00"));

                // touch the first key a minute later, should not move it forward.
                var result = cache.Get(items.First().Key, DateTime.Parse("2015-09-21 08:02:00"), true, true);
                Assert.Equal(items.First().Value, result);

                using var dataBlock = cache.Get(items.Keys.ToList(), DateTime.Parse("2015-09-21 09:01:00"), true, true);
                // request both keys, both should be expired 
                Assert.Equal(0, dataBlock.Data.Length);

                Assert.Equal(1, cache.Stats.TotalHits);
                Assert.Equal(2, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(2, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void Delete()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value, default);
                var deleteStatus = cache.Delete(key, default);

                Assert.Equal(WriteResult.Success, deleteStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void Delete_NullKey()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value, default);

                var deleteStatus = cache.Delete(default(CacheKey), default);

                Assert.Equal(WriteResult.MissingKey, deleteStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void Delete_KeyDoesNotExist()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var key1 = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key1, value, default);

                var key2 = new CacheKey(new byte[] { 33 });

                var deleteStatus = cache.Delete(key2, default);

                Assert.Equal(WriteResult.KeyDoesNotExist, deleteStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void Delete_Expired_Sliding()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, false);
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value1, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var putStatus = cache.Delete(key, secondPutTime);

                Assert.Equal(WriteResult.KeyDoesNotExist, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
                Assert.Equal(0, cache.Stats.TotalCacheSize);
            }
        }

        [Fact]
        public void Delete_Expired_Absolute()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, true);
                var key = new CacheKey(new byte[] { 11 });
                var value1 = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value1, firstPutTime);

                var secondPutTime = firstPutTime.AddTicks(2);
                var putStatus = cache.Delete(key, secondPutTime);

                Assert.Equal(WriteResult.KeyDoesNotExist, putStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
                Assert.Equal(0, cache.Stats.TotalCacheSize);
            }
        }

        [Fact]
        public void DeleteMany()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                Assert.Equal(WriteResult.Success, cache.Put(items, default));

                var result = cache.Delete(items.Keys.ToList(), default);
                Assert.Equal(2, result);
                using var dataBlock2 = cache.Get(items.Keys.ToList(), default, true, true);
                Assert.Equal(0, dataBlock2.Data.Length);

                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(2, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
                Assert.Equal(0, cache.Stats.TotalCacheSize);
            }
        }

        [Fact]
        public void DeleteMany_SingleNullKey_Atomic()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var items1 = new Dictionary<string, ByteString>
                {
                    { "a", ByteString.CopyFrom(new byte[] { 1 }) },
                    { "b", ByteString.CopyFrom(new byte[] { 2 }) }
                };

                Assert.Equal(WriteResult.Success, cache.Put(items1, default));

                Assert.Equal(0, cache.Delete(new[] { "a", "" }, default));

                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);

                using var dataBlock = cache.Get(items1.Keys.ToList(), default, true, true);
                Assert.Equal(items1.Count, dataBlock.Data.Length);
                Assert.Equal(items1.First().Key, dataBlock.Data[0].Key);
                Assert.Equal(items1.First().Value, dataBlock.Data[0].Value);
                Assert.Equal(items1.Last().Key, dataBlock.Data[1].Key);
                Assert.Equal(items1.Last().Value, dataBlock.Data[1].Value);
            }
        }

        [Fact]
        public void DeleteMany_Update_Expired_Sliding()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, false);

                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Put(items.First().Key, items.First().Value, firstPutTime);
                var secondPutTime = firstPutTime.AddTicks(1);
                cache.Put(items.Last().Key, items.Last().Value, secondPutTime);

                var delTime = secondPutTime.AddTicks(1);
                var result = cache.Delete(items.Keys.ToList(), delTime);

                Assert.Equal(1, result);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
                Assert.Equal(0, cache.Stats.TotalCacheSize);

                var getTime = delTime;
                using var values = cache.Get(items.Keys.ToList(), getTime, true, true);
                Assert.Equal(0, values.Data.Length);
            }
        }

        [Fact]
        public void DeleteMany_Update_Expired_Absolute()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, true);

                var items = new Dictionary<string, ByteString>
                {
                    { new CacheKey(new byte[] { 11 }), ByteString.CopyFrom(new byte[] { 1 }) },
                    { new CacheKey(new byte[] { 22 }), ByteString.CopyFrom(new byte[] { 2 }) }
                };

                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Put(items.First().Key, items.First().Value, firstPutTime);
                var secondPutTime = firstPutTime.AddTicks(1);
                cache.Put(items.Last().Key, items.Last().Value, secondPutTime);

                var delTime = secondPutTime.AddTicks(1);
                var result = cache.Delete(items.Keys.ToList(), delTime);

                Assert.Equal(1, result);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
                Assert.Equal(0, cache.Stats.TotalCacheSize);

                var getTime = delTime;
                using var values = cache.Get(items.Keys.ToList(), getTime, true, true);
                Assert.Equal(0, values.Data.Length);
            }
        }

        [Fact]
        public void Flush()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

            };

            using (var cache = GetPartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = ByteString.CopyFrom(new byte[] { 22 });
                cache.Put(key, value, default);

                cache.Flush();

                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }


        [Fact]
        public void TryIncrementValue()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var key = new CacheKey(new byte[] { 11 });
                var result = cache.TryIncrementValue(key, 1, default, out var value);

                Assert.Equal(WriteResult.Success, result);
                Assert.Equal(1, value);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(1, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(9, cache.Stats.TotalCacheSize);
            }
        }

        [Fact]
        public void TryIncrementValue_Exists()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var key = new CacheKey(new byte[] { 11 });
                var result = cache.TryIncrementValue(key, 1, default, out var value);

                Assert.Equal(WriteResult.Success, result);
                Assert.Equal(1, value);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(1, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(9, cache.Stats.TotalCacheSize);

                result = cache.TryIncrementValue(key, 10, default, out value);

                Assert.Equal(WriteResult.Success, result);
                Assert.Equal(11, value);
                Assert.Equal(1, cache.Stats.TotalHits);
                Assert.Equal(1, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(9, cache.Stats.TotalCacheSize);
            }
        }

        [Fact]
        public void TryIncrementValue_KeyTooLarge()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 1024,
            };

            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var key = new CacheKey(Helpers.BuildRandomByteArray(1500));
                var result = cache.TryIncrementValue(key, 1, default, out var value);

                Assert.Equal(WriteResult.KeyTooLarge, result);
                Assert.Equal(0, value);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
                Assert.Equal(0, cache.Stats.TotalCacheSize);
                Assert.Null(cache.Get(key, default, true, true));
            }
        }

        [Fact]
        public void TryIncrementValue_Expired_Sliding()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, false);
                var key = new CacheKey(new byte[] { 11 });
                var result = cache.TryIncrementValue(key, 1, firstPutTime, out var value);

                var secondPutTime = firstPutTime.AddTicks(2);
                result = cache.TryIncrementValue(key, 1, secondPutTime, out value);

                Assert.Equal(WriteResult.Success, result);
                Assert.Equal(1, value);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(2, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(9, cache.Stats.TotalCacheSize);
            }
        }

        [Fact]
        public void TryIncrementValue_Expired_Absolute()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var firstPutTime = DateTime.Parse("3:00:00 PM");
                cache.Metadata.SetExpiration(TimeSpan.FromTicks(2).Ticks, true);
                var key = new CacheKey(new byte[] { 11 });
                var result = cache.TryIncrementValue(key, 1, firstPutTime, out var value);

                var secondPutTime = firstPutTime.AddTicks(2);
                result = cache.TryIncrementValue(key, 1, secondPutTime, out value);

                Assert.Equal(WriteResult.Success, result);
                Assert.Equal(1, value);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(2, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(9, cache.Stats.TotalCacheSize);
            }
        }

        [Fact]
        public void TryIncrementValue_InsufficientCapacity()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                cache.Metadata.SetSize(2);
                cache.Metadata.SetEvictionPolicy(EvictionPolicy.NoEviction);
                var key = new CacheKey(new byte[] { 11 });
                var result = cache.TryIncrementValue(key, 1, default, out var value);

                Assert.Equal(WriteResult.InsufficientCapacity, result);
                Assert.Equal(0, value);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
                Assert.Equal(0, cache.Stats.TotalCacheSize);
                Assert.Null(cache.Get(key, default, true, true));
            }
        }

        [Fact]
        public void TryIncrementValue_InvalidTypeOperation()
        {
            using (var cache = GetPartition(new CacheServerOptions()))
            {
                var key = new CacheKey(new byte[] { 11 });
                var putStatus = cache.Put(key, ByteString.CopyFromUtf8("n"), default);

                Assert.Equal(WriteResult.Success, putStatus);

                var result = cache.TryIncrementValue(key, 10, default, out var value);

                Assert.Equal(WriteResult.InvalidTypeOperation, result);
                var getValue = cache.Get(key, default, true, true);

                Assert.Equal(ByteString.CopyFromUtf8("n"), getValue.Value);
                Assert.Equal(0, value);
                Assert.Equal(1, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
            }
        }


        public MemoryStorePartition GetPartition(CacheServerOptions options)
        {
            _mockClock
               .SetupGet(x => x.UtcNow)
               .Returns(DateTime.Parse("2019-04-25 3:00 PM").ToUniversalTime());

            var meta = new PartitionMetadata(
                _mockClock.Object.UtcNow.UtcDateTime.Ticks,
                0,
                false,
                false,
                EvictionPolicy.Lru,
                (long)options.MaxCacheSize);
            return new MemoryStorePartition(
                options,
                new PartitionStatistics(meta, new RuntimeStatistics(options, _mockClock.Object), _mockClock.Object.UtcNow.UtcDateTime),
                meta,
                EvictionPolicyFactory.GetEvictionPolicy(EvictionPolicy.Lru));
        }
    }
}
