using System.Collections.Generic;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Server.Runtime;

namespace Mackerel.RemoteCache.Server.Persistence
{
    public interface IPartitionStorage
    {
        Task Persist(string partitionKey, MemoryStorePartition partition);
        void Delete(string partitionKey);
        IAsyncEnumerable<KeyValuePair<CacheKey, PartitionMetadata>> RecoverMetaData();
    }
}
