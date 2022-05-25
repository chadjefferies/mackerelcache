using Mackerel.RemoteCache.Server.Runtime;

namespace Mackerel.RemoteCache.Server.Watch
{
    public readonly struct ChangeEvent
    {
        public CacheKey WatchId { get; }
        public CacheKey PartitionKey { get; }
        public CacheKey Key { get; }
        public byte[] Value { get; }
        public WatchFilterFlags EventType { get; }

        public ChangeEvent(CacheKey watchId, CacheKey partitionKey, CacheKey key, byte[] value, WatchFilterFlags eventType)
        {
            WatchId = watchId;
            PartitionKey = partitionKey;
            Key = key;
            Value = value;
            EventType = eventType;
        }
    }
}
