using System.Text.Json.Serialization;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Runtime;

namespace Mackerel.RemoteCache.Server.Persistence
{
    public class PartitionMetadataStorageRow
    {
        [JsonPropertyName("cd")]
        public long CreateDate { get; set; }
        [JsonPropertyName("exp")]
        public long ExpirationTicks { get; set; }
        [JsonPropertyName("abs")]
        public bool IsAbsoluteExpiration { get; set; }
        [JsonPropertyName("ee")]
        public EvictionPolicy EvictionPolicy { get; set; }
        [JsonPropertyName("mcs")]
        public long MaxCacheSize { get; set; }

        public PartitionMetadataStorageRow() { }

        public PartitionMetadataStorageRow(PartitionMetadata metadata)
        {
            CreateDate = metadata.CreateDate;
            ExpirationTicks = metadata.ExpirationTicks;
            IsAbsoluteExpiration = metadata.IsAbsoluteExpiration;
            EvictionPolicy = metadata.EvictionPolicy;
            MaxCacheSize = metadata.MaxCacheSize;
        }

        public PartitionMetadata ToMetaData() => new PartitionMetadata(CreateDate, ExpirationTicks, IsAbsoluteExpiration, true, EvictionPolicy, MaxCacheSize);
    }
}
