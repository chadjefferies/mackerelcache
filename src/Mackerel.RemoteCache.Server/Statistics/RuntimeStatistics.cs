using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Mackerel.RemoteCache.Server.Util;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Options;

namespace Mackerel.RemoteCache.Server.Statistics
{
    public class RuntimeStatistics : IDisposable
    {
        private readonly object _statsMutex = new object();
        private readonly CacheServerOptions _conf;
        private readonly ISystemClock _systemClock;

        private long _startTime;
        private long _evictedTime;
        private long _currentItemCount;
        private long _totalItemCount;
        private long _getHits;
        private long _getMisses;
        private long _evictions;
        private long _expirations;
        private int _partitions;

        private int _watches;
        private int _watchStreams;
        private long _watchEvents;
        private long _totalCacheSize;
        private long _totalReservedCacheSize;

        public long CurrentItems => _currentItemCount;
        public long TotalItems => _totalItemCount;
        public long Hits => _getHits;
        public long Misses => _getMisses;
        public long TotalEvictions => _evictions;
        public long TotalExpirations => _expirations;
        public long UptimeMs => (_systemClock.UtcNow.UtcDateTime.Ticks - _startTime) / TimeSpan.TicksPerMillisecond;
        public long ModifiedDate { get; private set; }
        public int Partitions => _partitions;
        public long MemoryUsed { get; private set; }
        public long AllocatedMemory { get; internal set; }
        public int PID { get; private set; }
        public long ServerTotalMemory { get; private set; }
        public long PagedSystemMemorySize { get; private set; }
        public long PagedMemorySize { get; private set; }
        public long VirtualMemorySize { get; private set; }
        public long WorkingSet { get; private set; }
        public long PeakPagedMemorySize { get; private set; }
        public long PeakVirtualMemorySize { get; private set; }
        public long PeakWorkingSet { get; private set; }
        public string ServerName { get; private set; }
        public int ServerProcessors { get; private set; }
        public long TotalProcessorTimeMs { get; private set; }
        public int GarbageCollections { get; internal set; }
        public long EvictedTimeMs => Volatile.Read(ref _evictedTime) / TimeSpan.TicksPerMillisecond;
        public int CurrentWatches => _watches;
        public int CurrentWatchStreams => _watchStreams;
        public long TotalWatchEvents => _watchEvents;
        public long TotalCacheSize => _totalCacheSize;
        public long TotalReservedCacheSize => _totalReservedCacheSize;
        public long[] HeapSizes { get; private set; } = new long[5];
        public long HeapFragmentation { get; private set; }
        public long TotalHeapSize { get; private set; }
        public double GcPauseTimePercentage { get; private set; }
        public long HeapCommitted { get; private set; }

        public double AvailableCapacity =>
            _conf.IsUnboundedCache ? long.MaxValue : _conf.MaxCacheSize - Math.Max(TotalCacheSize, TotalReservedCacheSize);

        public RuntimeStatistics(IOptions<CacheServerOptions> conf, ISystemClock systemClock)
        {
            _conf = conf.Value;
            _systemClock = systemClock;
        }

        public void Crunch()
        {
            lock (_statsMutex)
            {
                using var process = Process.GetCurrentProcess();

                MemoryUsed = process.PrivateMemorySize64;
                AllocatedMemory = GC.GetTotalMemory(false);
                GarbageCollections = GC.CollectionCount(2);

                var gcInfo = GC.GetGCMemoryInfo();
                ServerTotalMemory = gcInfo.TotalAvailableMemoryBytes;
                HeapFragmentation = gcInfo.FragmentedBytes;
                TotalHeapSize = gcInfo.HeapSizeBytes;
                GcPauseTimePercentage = gcInfo.PauseTimePercentage;
                HeapCommitted = gcInfo.TotalCommittedBytes;

                for (int i = 0; i < HeapSizes.Length; i++)
                {
                    HeapSizes[i] = gcInfo.GenerationInfo[i].SizeAfterBytes;
                }

                PagedSystemMemorySize = process.PagedSystemMemorySize64;
                PagedMemorySize = process.PagedMemorySize64;
                VirtualMemorySize = process.VirtualMemorySize64;
                WorkingSet = process.WorkingSet64;
                PeakPagedMemorySize = process.PeakPagedMemorySize64;
                PeakVirtualMemorySize = process.PeakVirtualMemorySize64;
                PeakWorkingSet = process.PeakWorkingSet64;
                TotalProcessorTimeMs = (long)process.TotalProcessorTime.TotalMilliseconds;

                ModifiedDate = _systemClock.UtcNow.UtcTicks;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementGetHits()
        {
            Interlocked.Increment(ref _getHits);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementGetHitsBy(int i)
        {
            Interlocked.Add(ref _getHits, i);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementGetMisses()
        {
            Interlocked.Increment(ref _getMisses);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementGetMissesBy(int i)
        {
            Interlocked.Add(ref _getMisses, i);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementCount()
        {
            Interlocked.Increment(ref _currentItemCount);
            Interlocked.Increment(ref _totalItemCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DecrementCount()
        {
            Interlocked.Decrement(ref _currentItemCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementCountBy(long i)
        {
            Interlocked.Add(ref _currentItemCount, i);
            Interlocked.Add(ref _totalItemCount, i);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DecrementCountBy(long i)
        {
            Interlocked.Add(ref _currentItemCount, -i);
        }

        public void IncrementEvictions()
        {
            Interlocked.Increment(ref _evictions);
        }

        public void IncrementEvictionCountBy(long i)
        {
            Interlocked.Add(ref _evictions, i);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementExpirations()
        {
            Interlocked.Increment(ref _expirations);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementExpirationCountBy(long i)
        {
            Interlocked.Add(ref _expirations, i);
        }

        public void ResetCount()
        {
            Interlocked.Exchange(ref _currentItemCount, 0);
        }

        public void IncrementPartitions()
        {
            Interlocked.Increment(ref _partitions);
        }

        public void DecrementPartitions()
        {
            Interlocked.Decrement(ref _partitions);
        }

        public void ResetPartitionCount()
        {
            Interlocked.Exchange(ref _partitions, 0);
        }

        public void SetEvictedTime(long evictedTime)
        {
            Volatile.Write(ref _evictedTime, evictedTime);
        }

        public void IncrementWatches()
        {
            Interlocked.Increment(ref _watches);
        }

        public void DecrementWatches()
        {
            Interlocked.Decrement(ref _watches);
        }

        public void DecrementWatchesBy(int i)
        {
            Interlocked.Add(ref _watches, -i);
        }

        public void IncrementWatchStreams()
        {
            Interlocked.Increment(ref _watchStreams);
        }

        public void DecrementWatchStreams()
        {
            Interlocked.Decrement(ref _watchStreams);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementWatchEvents()
        {
            Interlocked.Increment(ref _watchEvents);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementSize(long i)
        {
            Interlocked.Add(ref _totalCacheSize, i);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DecrementSize(long i)
        {
            Interlocked.Add(ref _totalCacheSize, -i);
        }

        public void IncrementReservedSize(long i)
        {
            Interlocked.Add(ref _totalReservedCacheSize, i);
        }

        public void DecrementReservedSize(long i)
        {
            Interlocked.Add(ref _totalReservedCacheSize, -i);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ShouldEvict() =>
            !_conf.IsUnboundedCache && _totalCacheSize > _conf.MaxCacheSize;

        public void Dispose()
        {

        }

        public static double CalculateHitRate(long hits, long misses)
        {
            if (hits == 0 && misses == 0)
            {
                return 0.0;
            }

            return Math.Round(hits / Convert.ToDouble(hits + misses), 4);
        }

        public static RuntimeStatistics Create(IOptions<CacheServerOptions> conf, ISystemClock systemClock)
        {
            using var process = Process.GetCurrentProcess();

            return new RuntimeStatistics(conf, systemClock)
            {
                PID = process.Id,
                ServerName = Environment.MachineName,
                ServerProcessors = Environment.ProcessorCount,
                _startTime = process.StartTime.ToUniversalTime().Ticks,
                ModifiedDate = systemClock.UtcNow.UtcTicks
            };
        }
    }
}
