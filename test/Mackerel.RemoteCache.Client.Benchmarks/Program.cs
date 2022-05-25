using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Mackerel.RemoteCache.Client.Encoding;
using Mackerel.RemoteCache.Client.Routing;

namespace Mackerel.RemoteCache.Client.Benchmarks
{
    class Program
    {
        static void Main()
        {
            Thread.Sleep(2000);
            var summary = BenchmarkRunner.Run(typeof(Program).Assembly);
        }
    }

    [InProcess]
    [MemoryDiagnoser]
    public class IncrementClientBenchmarks
    {
        const string partitionKey = nameof(IncrementClientBenchmarks);

        ICacheConnection _connection;
        ICache<string> _cache;

        [GlobalSetup]
        public async Task Setup()
        {
            _connection = CacheConnection.Create("localhost");
            _cache = _connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(_connection), new KeyRouter());

            // fill up the cache

            for (int j = 0; j < 1000000; j++)
            {
                await _cache.PutAsync(partitionKey,
                    string.Join("_", Enumerable.Repeat("key" + j, 5)),
                    string.Join("_", Enumerable.Repeat("value" + j, 5)));
            }
        }

        [GlobalCleanup]
        public async ValueTask Cleanup()
        {
            await _connection.DisposeAsync();
        }

        string _cacheIncrementKey = Guid.NewGuid().ToString("N");
        [Benchmark]
        public async Task CacheIncrementAsync()
        {
            await _cache.IncrementAsync(partitionKey, _cacheIncrementKey);
        }

        [Benchmark]
        public async Task CacheIncrementByAsync5K()
        {
            var k = Guid.NewGuid().ToString("N");
            await _cache.IncrementByAsync(partitionKey, k, 5000);
        }
    }

    [InProcess]
    [MemoryDiagnoser]
    public class SimilarKeyClientBenchmarks
    {
        const string partitionKey = nameof(SimilarKeyClientBenchmarks);

        Dictionary<string, string> _keyValues;
        ICacheConnection _connection;
        ICache<string> _cache;

        [GlobalSetup]
        public async Task Setup()
        {
            _connection = CacheConnection.Create("localhost");
            _cache = _connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(_connection), new KeyRouter());

            _keyValues = new Dictionary<string, string>(Enumerable.Range(0, 5000).Select(x =>
                new KeyValuePair<string, string>(
                    string.Join("_", Enumerable.Repeat("key" + x, 5)),
                    string.Join("_", Enumerable.Repeat("value" + x, 5)))));

            // fill up the cache
            await _cache.PutAsync(partitionKey, _keyValues);
        }

        [GlobalCleanup]
        public async ValueTask Cleanup()
        {
            await _connection.DisposeAsync();
        }

        [Benchmark]
        public Task CacheGet5K()
        {
            return _cache.GetAsync(partitionKey, _keyValues.Keys);
        }

        [Benchmark]
        public Task CachePut5K()
        {
            return _cache.PutAsync(partitionKey, _keyValues);
        }
    }

    [InProcess]
    [MemoryDiagnoser]
    public class RandomKeyClientBenchmarks
    {
        const string partitionKey = nameof(RandomKeyClientBenchmarks);

        Dictionary<string, string> _keyValues;
        ICacheConnection _connection;
        ICache<string> _cache;

        [GlobalSetup]
        public async Task Setup()
        {
            _connection = CacheConnection.Create("localhost");
            _cache = _connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(_connection), new KeyRouter());

            _keyValues = new Dictionary<string,string>(Enumerable.Range(0, 5000).Select(x =>
                new KeyValuePair<string, string>(
                    Guid.NewGuid().ToString("N"),
                    string.Join("_", Enumerable.Repeat("value" + x, 5)))));

            // fill up the cache
            await _cache.PutAsync(partitionKey, _keyValues);
        }

        [GlobalCleanup]
        public async Task Cleanup()
        {
            await _connection.DisposeAsync();
        }

        [Benchmark]
        public Task CacheGet5K()
        {
            return _cache.GetAsync(partitionKey, _keyValues.Keys);
        }

        [Benchmark]
        public Task CachePut5K()
        {
            return _cache.PutAsync(partitionKey, _keyValues);
        }
    }
}
