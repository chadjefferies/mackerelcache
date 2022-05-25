using System;

namespace Mackerel.RemoteCache.Client.Encoding
{
    /// <summary>
    /// Built-in UTF8 string codec.
    /// </summary>
    public class StringCacheCodec : ICacheCodec<string>
    {
        private static System.Text.Encoding Encoding => System.Text.Encoding.UTF8;

        public string Decode(ReadOnlySpan<byte> value, bool isNull)
        {
            if (isNull) return null;
            return Encoding.GetString(value);
        }

        public ReadOnlySpan<byte> Encode(string value)
        {
            if (value == null) return default;
            return Encoding.GetBytes(value);
        }
    }
}
