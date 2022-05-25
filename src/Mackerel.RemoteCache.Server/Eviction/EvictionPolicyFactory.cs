using System;
using Mackerel.RemoteCache.Api.V1;

namespace Mackerel.RemoteCache.Server.Eviction
{
    public static class EvictionPolicyFactory
    {
        public static IEvictionPolicy GetEvictionPolicy(EvictionPolicy evictionPolicy)
        {
            switch (evictionPolicy)
            {
                case EvictionPolicy.NoEviction:
                    return new NoEvictionPolicy();
                case EvictionPolicy.Lru:
                    return new RandomLruEvictionPolicy();
                default:
                    throw new NotImplementedException();
            }

        }
    }
}
