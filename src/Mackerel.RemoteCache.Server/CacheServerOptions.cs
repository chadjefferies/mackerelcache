using System;
using System.Diagnostics.CodeAnalysis;
using Mackerel.RemoteCache.Server.Util;
using Microsoft.Extensions.Options;

namespace Mackerel.RemoteCache.Server
{
    [ExcludeFromCodeCoverage]
    public class CacheServerOptions : IOptions<CacheServerOptions>
    {
        public double MaxCacheSize { get; set; } = 0;
        
        public TimeSpan StatsInterval { get; set; } = TimeSpan.FromMinutes(1);

        public TimeSpan EagerExpirationInterval { get; set; } = TimeSpan.FromSeconds(10);
        public TimeSpan EagerExpirationJobLimit { get; set; } = TimeSpan.FromSeconds(10);
        public int KeyExpirationSamples { get; set; } = 20;

        public int KeyEvictionSamples { get; set; } = 5;
        public double EvictionSampleRate { get; set; } = .01;

        public int MaxBytesPerKey { get; set; } = 512;
        public int MaxBytesPerValue { get; set; } = 1048576;

        public string DataLocation { get; set; } = "./Data";

        public bool IsUnboundedCache => MaxCacheSize == 0;

        public CacheServerOptions Value => this;

        public bool Validate()
        {
            if (MaxCacheSize > 0)
            {
                if (MaxCacheSize < 1.0)
                {
                    MaxCacheSize = (MemoryStatus.GetTotalPhysicalMemory() * MaxCacheSize);
                }
                else if (MaxCacheSize >= 1) // somewhat arbitrary, but we need a limit.
                {
                    MaxCacheSize = ByteSize.FromUnit(Convert.ToInt32(MaxCacheSize), ByteSize.Unit.MB);
                }
                else
                {
                    throw new ArgumentOutOfRangeException(nameof(MaxCacheSize), $"{nameof(MaxCacheSize)} cannot be less than 1 MB");
                }
            }

            return true;
        }
    }
}
