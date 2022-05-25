using System;
using Mackerel.RemoteCache.Api.V1;

namespace Mackerel.RemoteCache.Server.Watch
{
    /// <summary>
    /// Needs maintained alongside protobuf type <see cref="WatchEventType"/>
    /// </summary>
    /// <remarks>
    /// protobuf doesn't support bit field enums
    /// </remarks>
    [Flags]
    public enum WatchFilterFlags
    {
        None = 0,
        Write = 1,
        Delete = 2,
        Evict = 4,
        Expire = 8
    }

    public static class WatchFilterFlagsExtensions
    {
        public static WatchEventType ToWatchEventType(this WatchFilterFlags flags)
        {
            return flags switch
            {
                WatchFilterFlags.Write => WatchEventType.Write,
                WatchFilterFlags.Delete => WatchEventType.Delete,
                WatchFilterFlags.Evict => WatchEventType.Evict,
                WatchFilterFlags.Expire => WatchEventType.Expire,
                _ => throw new NotSupportedException(),
            };
        }

        public static WatchFilterFlags ToWatchFilterFlags(this WatchEventType eventType)
        {
            return eventType switch
            {
                WatchEventType.Write => WatchFilterFlags.Write,
                WatchEventType.Delete => WatchFilterFlags.Delete,
                WatchEventType.Evict => WatchFilterFlags.Evict,
                WatchEventType.Expire => WatchFilterFlags.Expire,
                _ => throw new NotSupportedException(),
            };
        }
    }
}
