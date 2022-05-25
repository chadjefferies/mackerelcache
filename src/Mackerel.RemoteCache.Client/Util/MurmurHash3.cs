using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Mackerel.RemoteCache.Client.Util
{
    // https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp

    internal static class MurmurHash3
    {
        public static ulong Hash128(ReadOnlySpan<byte> data, uint seed = 42)
        {
            const ulong c1 = 0x87c37b91114253d5L;
            const ulong c2 = 0x4cf5ad432745937fL;

            var length = data.Length;
            var h1 = (ulong)seed;
            var h2 = (ulong)seed;

            //----------
            // body
            ulong k1;
            ulong k2;

            ReadOnlySpan<ulong> blocks = MemoryMarshal.Cast<byte, ulong>(data);

            var numBlocks = length >> 4;
            var index = 0;

            while (numBlocks-- > 0)
            {
                k1 = blocks[index++];
                k2 = blocks[index++];

                k1 *= c1; k1 = Rotl64(k1, 31); k1 *= c2; h1 ^= k1;
                h1 = Rotl64(h1, 27); h1 += h2; h1 = (h1 * 5) + 0x52dce729L;

                k2 *= c2; k2 = Rotl64(k2, 33); k2 *= c1; h2 ^= k2;
                h2 = Rotl64(h2, 31); h2 += h1; h2 = (h2 * 5) + 0x38495ab5L;
            }

            //----------
            // tail
            k1 = 0;
            k2 = 0;
            var tail = data.Slice(length & (~15));

            switch (tail.Length)
            {
                case 15: k2 ^= (ulong)tail[14] << 48; goto case 14;
                case 14: k2 ^= (ulong)tail[13] << 40; goto case 13;
                case 13: k2 ^= (ulong)tail[12] << 32; goto case 12;
                case 12: k2 ^= (ulong)tail[11] << 24; goto case 11;
                case 11: k2 ^= (ulong)tail[10] << 16; goto case 10;
                case 10: k2 ^= (ulong)tail[09] << 08; goto case 9;
                case 9:
                    k2 ^= tail[8];
                    k2 *= c2; k2 = Rotl64(k2, 33); k2 *= c1; h2 ^= k2;
                    goto case 8;
                case 8: k1 ^= (ulong)tail[7] << 56; goto case 7;
                case 7: k1 ^= (ulong)tail[6] << 48; goto case 6;
                case 6: k1 ^= (ulong)tail[5] << 40; goto case 5;
                case 5: k1 ^= (ulong)tail[4] << 32; goto case 4;
                case 4: k1 ^= (ulong)tail[3] << 24; goto case 3;
                case 3: k1 ^= (ulong)tail[2] << 16; goto case 2;
                case 2: k1 ^= (ulong)tail[1] << 08; goto case 1;
                case 1:
                    k1 ^= tail[0];
                    k1 *= c1; k1 = Rotl64(k1, 31); k1 *= c2; h1 ^= k1;
                    break;
            }

            //----------
            // finalization

            h1 ^= (ulong)length; h2 ^= (ulong)length;

            h1 += h2;
            h2 += h1;

            h1 = Fmix64(h1);
            h2 = Fmix64(h2);

            h1 += h2;

            return h1;
        }

        //-----------------------------------------------------------------------------
        // Finalization mix - force all bits of a hash block to avalanche

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static ulong Fmix64(ulong h)
        {
            h ^= h >> 33;
            h *= 0xff51afd7ed558ccd;
            h ^= h >> 33;
            h *= 0xc4ceb9fe1a85ec53;
            h ^= h >> 33;

            return h;
        }

        //-----------------------------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static ulong Rotl64(ulong x, int r) => (x << r) | (x >> (64 - r));
    }
}
