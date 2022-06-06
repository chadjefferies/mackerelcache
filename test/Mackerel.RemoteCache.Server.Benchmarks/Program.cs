using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Caching;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using FASTER.core;
using Google.Protobuf;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Persistence;
using Mackerel.RemoteCache.Server.Rpc;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Watch;
using Microsoft.Extensions.Internal;

namespace Mackerel.RemoteCache.Server.Benchmarks
{
    class Program
    {
        static void Main()
        {
            try
            {
                var summary = BenchmarkRunner.Run(typeof(Program).Assembly);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }

    [InProcess]
    [MemoryDiagnoser]
    public class CacheServerBenchmarks
    {
        const string partitionKey = nameof(CacheServerBenchmarks);

        Dictionary<string, ByteString> _keyValues;
        IReadOnlyList<string> _keys;

        MemoryStore _memStore;
        MackerelCacheServiceHandler _serverHandler;

        [GlobalSetup]
        public void Setup()
        {
            var opt = new CacheServerOptions();
            var stats = new Statistics.RuntimeStatistics(opt, new SystemClock());
            _memStore = new MemoryStore(
               null, 
               opt,
               stats,
               new FileSystemPartitionStorage(opt));
            _serverHandler = new MackerelCacheServiceHandler(_memStore, new SystemClock(), Quartz.Impl.DirectSchedulerFactory.Instance, null);

            _keyValues = new Dictionary<string, ByteString>(Enumerable.Range(0, 5000).Select(x =>
            new KeyValuePair<string, ByteString>(
                string.Join("_", Enumerable.Repeat("key" + x, 5)),
                ByteString.CopyFromUtf8(
                    string.Join("_", Enumerable.Repeat("value" + x, 12))))));

            _keys = _keyValues.Keys.ToList();
            // fill up the cache
            _memStore.Put(partitionKey, _keyValues, DateTime.UtcNow);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _memStore.Dispose();
        }

        [Benchmark]
        public async Task RemoteCache_SinglePartition_Get5K_Rpc()
        {
            var request = new GetManyRequest
            {
                PartitionKey = partitionKey
            };

            for (int j = 0; j < _keys.Count; j++)
            {
                request.Keys.Add(_keys[j]);
            }

            var r = await _serverHandler.GetMany(request, null);
        }

        [Benchmark]
        public void MemoryStore_SinglePartition_Get5K()
        {
            using var r = _memStore.Get(partitionKey, _keys, DateTime.UtcNow);
        }

        [Benchmark]
        public void RemoteCache_SinglePartition_Put5k_Rpc()
        {
            var request = new PutManyRequest
            {
                PartitionKey = partitionKey
            };

            foreach (var kv in _keyValues)
            {
                request.Entries.Add(kv.Key, kv.Value);
            }

            _serverHandler.PutMany(request, null);
        }
    }

    //    [InProcess]
    //    [MemoryDiagnoser]
    //    public class FASTERBenchmarks
    //    {
    //        static readonly CacheKey[] keys = Enumerable.Range(0, 1000).Select(x =>
    //            new CacheKey(System.Text.Encoding.UTF8.GetBytes(
    //                string.Join("_", Enumerable.Repeat("key" + x, 5))))).ToArray();

    //        static readonly KeyValuePair<CacheKey, CacheValue>[] keyValues = Enumerable.Range(0, 1000).Select(x =>
    //            new KeyValuePair<CacheKey, CacheValue>(
    //                new CacheKey(System.Text.Encoding.UTF8.GetBytes(
    //                string.Join("_", Enumerable.Repeat("key" + x, 5)))),
    //                new CacheValue(System.Text.Encoding.UTF8.GetBytes(
    //                string.Join("_", Enumerable.Repeat("value" + x, 12))), DateTime.UtcNow))).ToArray();

    //        FasterKV<CacheKey, CacheValue, CacheValue, CacheValue, Empty, Funcs> _cache;
    //        IDevice _log;

    //        [GlobalSetup]
    //        public void Setup()
    //        {
    //            _log = Devices.CreateLogDevice(null);
    //            _cache = new FasterKV<CacheKey, CacheValue, CacheValue, CacheValue, Empty, Funcs>
    //              (
    //                size: 1L << 20,
    //                functions: new Funcs(),
    //                logSettings: new LogSettings
    //                {
    //                    LogDevice = _log
    //                },
    //                serializerSettings: new SerializerSettings<CacheKey, CacheValue>
    //                {
    //                    valueSerializer = () => new CacheValueSerializer()
    //                });
    //            //comparer: new CacheKeyEqualityComparer());
    //            _cache.StartSession();

    //            // fill up the cache
    //            var timestamp = DateTime.UtcNow;
    //            for (int j = 0; j < 1000000; j++)
    //            {
    //                var k = new CacheKey(System.Text.Encoding.UTF8.GetBytes(
    //                    string.Join("_", Enumerable.Repeat("key" + j, 5))));
    //                var v = new CacheValue(System.Text.Encoding.UTF8.GetBytes(
    //                    string.Join("_", Enumerable.Repeat("value" + j, 12))), DateTime.UtcNow);
    //                _cache.Upsert(ref k, ref v, Empty.Default, 0);
    //            }

    //            _cache.StopSession();
    //        }

    //        [GlobalCleanup]
    //        public void Cleanup()
    //        {

    //            _cache.Dispose();
    //            _log.Close();
    //        }

    //        [Benchmark]
    //        public void SingleGet()
    //        {
    //            var sessionUID = _cache.StartSession();

    //            for (int i = 0; i < 1000000; i++)
    //            {
    //                var input = new CacheValue();
    //                var output = new CacheValue();
    //                var status = _cache.Read(ref keys[i % keys.Length], ref input, ref output, Empty.Default, 0);
    //            }

    //            _cache.StopSession();
    //        }

    //        [Benchmark]
    //        public void SinglePut()
    //        {
    //            _cache.StartSession();
    //            var now = DateTime.UtcNow;

    //            for (int i = 0; i < 1000000; i++)
    //            {
    //                var kv = keyValues[i % keyValues.Length];
    //                var k = kv.Key;
    //                var v = kv.Value;
    //                var status = _cache.Upsert(ref k, ref v, Empty.Default, 0);
    //            }

    //            _cache.StopSession();
    //        }

    //        public class Funcs : IFunctions<CacheKey, CacheValue, CacheValue, CacheValue, Empty>
    //        {
    //            public void SingleReader(ref CacheKey key, ref CacheValue input, ref CacheValue value, ref CacheValue dst) => dst = value;
    //            public void SingleWriter(ref CacheKey key, ref CacheValue src, ref CacheValue dst) => dst = src;
    //            public void ConcurrentReader(ref CacheKey key, ref CacheValue input, ref CacheValue value, ref CacheValue dst) => dst = value;
    //            public void ConcurrentWriter(ref CacheKey key, ref CacheValue src, ref CacheValue dst) => dst = src;
    //            public void InitialUpdater(ref CacheKey key, ref CacheValue input, ref CacheValue value) => value = input;
    //            public void CopyUpdater(ref CacheKey key, ref CacheValue input, ref CacheValue oldv, ref CacheValue newv) => newv = oldv;
    //            public void InPlaceUpdater(ref CacheKey key, ref CacheValue input, ref CacheValue value) => value = input;
    //            public void UpsertCompletionCallback(ref CacheKey key, ref CacheValue value, Empty ctx) { }
    //            public void ReadCompletionCallback(ref CacheKey key, ref CacheValue input, ref CacheValue output, Empty ctx, Status s) { }
    //            public void RMWCompletionCallback(ref CacheKey key, ref CacheValue input, Empty ctx, Status s) { }
    //            public void CheckpointCompletionCallback(Guid sessionId, long serialNum) { }
    //            public void DeleteCompletionCallback(ref CacheKey key, Empty ctx) { }
    //        }

    //        public class CacheValueSerializer : BinaryObjectSerializer<CacheValue>
    //        {
    //            public override void Deserialize(ref CacheValue obj)
    //            {
    //                throw new NotImplementedException();
    //            }

    //            public override void Serialize(ref CacheValue obj)
    //            {
    //                throw new NotImplementedException();
    //            }
    //        }

    //        public class CacheKeyEqualityComparer : IFasterEqualityComparer<CacheKey>
    //        {
    //            public bool Equals(ref CacheKey k1, ref CacheKey k2)
    //            {
    //                return k1.Equals(k2);
    //            }

    //            public long GetHashCode64(ref CacheKey k)
    //            {
    //                return k.GetHashCode();
    //            }
    //        }
    //    }
}
