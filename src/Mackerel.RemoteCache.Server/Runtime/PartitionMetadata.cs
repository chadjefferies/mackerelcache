using System.Threading;
using Mackerel.RemoteCache.Api.V1;

namespace Mackerel.RemoteCache.Server.Runtime
{
    /// <summary>
    /// Meta data that describes characteristics about a partition.
    /// </summary>
    /// <remarks>
    /// These fields have the ability to persist across restarts.
    /// </remarks>
    public class PartitionMetadata
    {
        private long _expirationTicks;
        private long _maxCacheSize;
        private volatile bool _absoluteExpiration;
        private volatile bool _isPersisted;
        private volatile EvictionPolicy _evictionPolicy;

        public long CreateDate { get; }
        public long ExpirationTicks => Volatile.Read(ref _expirationTicks);
        public bool IsAbsoluteExpiration => _absoluteExpiration;
        public EvictionPolicy EvictionPolicy => _evictionPolicy;
        public long MaxCacheSize => Volatile.Read(ref _maxCacheSize);

        public bool IsPersisted => _isPersisted;
        public bool IsUnboundedCache => MaxCacheSize == 0;

        public PartitionMetadata(long createDate, long expirationTicks, bool isAbsoluteExpiration, bool persist, EvictionPolicy evictionPolicy, long maxCacheSize)
        {
            _isPersisted = persist;
            CreateDate = createDate;
            _expirationTicks = expirationTicks;
            _absoluteExpiration = isAbsoluteExpiration;
            _evictionPolicy = evictionPolicy;
            _maxCacheSize = maxCacheSize;
        }

        public void SetExpiration(long expiry, bool absoluteExpiration)
        {
            Volatile.Write(ref _expirationTicks, expiry);
            _absoluteExpiration = absoluteExpiration;
        }

        public void SetEvictionPolicy(EvictionPolicy evictionPolicy)
        {
            _evictionPolicy = evictionPolicy;
        }

        public void SetSize(long size)
        {
            Volatile.Write(ref _maxCacheSize, size);
        }

        public void SetPersisted(bool isPersisted)
        {
            _isPersisted = isPersisted;
        }
    }
}
