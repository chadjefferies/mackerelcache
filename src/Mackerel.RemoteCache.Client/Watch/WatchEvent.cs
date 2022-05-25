using Mackerel.RemoteCache.Api.V1;

namespace Mackerel.RemoteCache.Client.Watch
{
    /// <summary>
    /// Represents a key change event.
    /// </summary>
    public readonly struct WatchEvent<T>
    {
        /// <summary>
        /// The unique id of this watcher
        /// </summary>
        public string WatchId { get; }

        /// <summary>
        /// The partition the key-value pair belongs to.
        /// </summary>
        public string Partition { get; }

        /// <summary>
        /// The key that was changed.
        /// </summary>
        public string Key { get; }

        /// <summary>
        /// The current value.
        /// </summary>
        public T Value { get; }

        /// <summary>
        /// The type of event that caused the change.
        /// </summary>
        public WatchEventType EventType { get; }

        public WatchEvent(string watchId, string partiton, string key, T value, WatchEventType eventType)
        {
            WatchId = watchId;
            Partition = partiton;
            Key = key;
            Value = value;
            EventType = eventType;
        }
    }
}
