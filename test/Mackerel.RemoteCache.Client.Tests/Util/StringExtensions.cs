using Google.Protobuf;

namespace Mackerel.RemoteCache.Client.Tests.Util
{
    public static class StringExtensions
    {
        public static byte[] ToByteArray(this string value)
            => System.Text.Encoding.UTF8.GetBytes(value);

        public static ByteString ToByteString(this string value)
            => ByteString.CopyFromUtf8(value);
    }
}
