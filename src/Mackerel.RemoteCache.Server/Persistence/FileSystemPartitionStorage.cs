using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Server.Runtime;
using Microsoft.Extensions.Options;

namespace Mackerel.RemoteCache.Server.Persistence
{
    public class FileSystemPartitionStorage : IPartitionStorage
    {
        private readonly CacheServerOptions _config;

        public FileSystemPartitionStorage(IOptions<CacheServerOptions> conf)
        {
            _config = conf.Value;
        }

        public async Task Persist(string partitionKey, MemoryStorePartition partition)
        {
            using FileStream fs = File.Create(Path.Join(_config.DataLocation, partitionKey));
            await JsonSerializer.SerializeAsync(fs, new PartitionMetadataStorageRow(partition.Metadata));
        }

        public void Delete(string partitionKey)
        {
            File.Delete(Path.Join(_config.DataLocation, partitionKey));
        }

        public async IAsyncEnumerable<KeyValuePair<CacheKey, PartitionMetadata>> RecoverMetaData()
        {
            foreach (string filePath in Directory.EnumerateFiles(_config.DataLocation))
            {
                PartitionMetadataStorageRow metadataStorage;
                using (FileStream fs = File.OpenRead(filePath))
                {
                    metadataStorage = await JsonSerializer.DeserializeAsync<PartitionMetadataStorageRow>(fs);
                }
                string fileName = Path.GetFileNameWithoutExtension(filePath);
                yield return new KeyValuePair<CacheKey, PartitionMetadata>(fileName, metadataStorage.ToMetaData());
            }
        }

        public static FileSystemPartitionStorage Create(IOptions<CacheServerOptions> conf)
        {
            Directory.CreateDirectory(conf.Value.DataLocation);
            return new FileSystemPartitionStorage(conf);
        }
    }
}
