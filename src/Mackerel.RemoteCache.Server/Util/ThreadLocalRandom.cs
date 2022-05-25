using System;
using System.Threading;

namespace Mackerel.RemoteCache.Server.Util
{
    public static class ThreadLocalRandom
    {
        private static readonly ThreadLocal<Random> _random = new ThreadLocal<Random>(() => new Random());

        public static Random Current
        {
            get
            {
                return _random.Value;
            }
        }
    }
}
