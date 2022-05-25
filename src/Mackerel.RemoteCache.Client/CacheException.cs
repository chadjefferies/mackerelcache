using System;
using Mackerel.RemoteCache.Api.V1;

namespace Mackerel.RemoteCache.Client
{
    public class CacheException : Exception
    {
        public WriteResult Result { get; }

        public CacheException(WriteResult result) : base(result.ToString())
        {
            Result = result;
        }
    }
}
