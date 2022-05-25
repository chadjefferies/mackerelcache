using System;
using System.Runtime;

namespace Mackerel.RemoteCache.Server.Util
{
    internal static class MemoryStatus
    {
        public static long GetTotalPhysicalMemory()
        {
            return GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        }

        public static long GetHighMemoryThreshold()
        {
            return GC.GetGCMemoryInfo().HighMemoryLoadThresholdBytes;
        }

        public static MemoryFailPoint GetMemoryFailPoint(long maxMemory)
        {
            return new MemoryFailPoint(Math.Max(1, ByteSize.ToUnit(maxMemory, ByteSize.Unit.MB)));
        }

        public static bool HasSufficientMemory(long maxMemory)
        {
            try
            {
                using (GetMemoryFailPoint(maxMemory))
                {
                    return true;
                }
            }
            catch (InsufficientMemoryException)
            {
                return false;
            }
        }
    }
}
