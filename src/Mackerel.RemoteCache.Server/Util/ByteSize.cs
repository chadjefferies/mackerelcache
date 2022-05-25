using System;

namespace Mackerel.RemoteCache.Server.Util
{
    public static class ByteSize
    {
        public enum Unit : int
        {
            KB = 1,
            MB = 2,
            GB = 3,
            TB = 4
        }

        public static int ToUnit(long value, Unit unit)
        {
            return Convert.ToInt32(value / Math.Pow(1024, (int)unit));
        }

        public static long FromUnit(int value, Unit unit)
        {
            return Convert.ToInt64(value * Math.Pow(1024, (int)unit));
        }
    }
}
