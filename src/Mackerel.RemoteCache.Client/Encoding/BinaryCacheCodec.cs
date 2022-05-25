using System;

namespace Mackerel.RemoteCache.Client.Encoding
{
    /// <summary>
    /// Built-in byte array codec.
    /// </summary>
    public class BinaryCacheCodec : ICacheCodec<byte[]>
    {
        public byte[] Decode(ReadOnlySpan<byte> value, bool isNull)
        {
            if (isNull) return null;

            return value.ToArray();
        }

        public ReadOnlySpan<byte> Encode(byte[] value) => value;
    }
}
