using System.Collections.Generic;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Server.Persistence;
using Mackerel.RemoteCache.Server.Runtime;

namespace Mackerel.RemoteCache.Server.Tests.Util
{
    public class StubMemoryStorage : IPartitionStorage
    {
        public void Delete(string partitionKey)
        {

        }

        public Task Persist(string partitionKey, MemoryStorePartition partition)
        {
            return Task.CompletedTask;
        }

        public async IAsyncEnumerable<KeyValuePair<CacheKey, PartitionMetadata>> RecoverMetaData()
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    public class MockMemoryStorage : IPartitionStorage
    {
        private Dictionary<CacheKey, PartitionMetadata> _storage = new Dictionary<CacheKey, PartitionMetadata>();

        public void Delete(string partitionKey)
        {
            _storage.Remove(partitionKey);
        }

        public Task Persist(string partitionKey, MemoryStorePartition partition)
        {
            _storage.Add(partitionKey, partition.Metadata);
            return Task.CompletedTask;
        }

        public async IAsyncEnumerable<KeyValuePair<CacheKey, PartitionMetadata>> RecoverMetaData()
        {
            foreach (var item in _storage)
            {
                yield return item;
            }

            await Task.CompletedTask;
        }
    }
}
