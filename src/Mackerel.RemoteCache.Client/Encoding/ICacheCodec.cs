using System;

namespace Mackerel.RemoteCache.Client.Encoding
{
    public interface ICacheCodec<T>
    {
        T Decode(ReadOnlySpan<byte> value, bool isNull);

        ReadOnlySpan<byte> Encode(T value);
    }
}