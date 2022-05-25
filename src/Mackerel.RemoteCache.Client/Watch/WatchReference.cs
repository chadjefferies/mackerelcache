using System;
using Mackerel.RemoteCache.Api.V1;

namespace Mackerel.RemoteCache.Client.Watch
{
    internal class WatchReference<T>: IEquatable<WatchReference<T>>
    {
        public WatchCreateRequest CreateRequest { get; }
        public Action<WatchEvent<T>> Handler { get; }

        public WatchReference(WatchCreateRequest createRequest, Action<WatchEvent<T>> handler)
        {
            CreateRequest = createRequest;
            Handler = handler;
        }

        public bool Equals(WatchReference<T> other)
        {
            return other.CreateRequest.WatchId == CreateRequest.WatchId;
        }

        public override bool Equals(object obj)
        {
            if (obj is WatchReference<T> k) return Equals(k);
            return Equals((WatchReference<T>)obj);
        }

        public override int GetHashCode() => CreateRequest.WatchId.GetHashCode();

        public static bool operator ==(WatchReference<T> x, WatchReference<T> y) => x.Equals(y);

        public static bool operator !=(WatchReference<T> x, WatchReference<T> y) => !(x == y);
    }
}
