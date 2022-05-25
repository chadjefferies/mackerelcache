using System;

namespace Mackerel.RemoteCache.Server.Tests.Util
{
    public static class Helpers
    {
        public static byte[] BuildRandomByteArray(int size)
        {
            var rand = new Random();
            var buffer = new byte[size];
            rand.NextBytes(buffer);
            return buffer;
        }
    }
}
