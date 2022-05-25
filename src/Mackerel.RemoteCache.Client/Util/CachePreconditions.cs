using System;

namespace Mackerel.RemoteCache.Client.Util
{
    internal static class CachePreconditions
    {
        public static void CheckNotNull<T>(T argument, string paramName)
        {
            if (argument == null)
            {
                throw new ArgumentNullException(paramName);
            }
        }
    }
}
