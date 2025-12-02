using System;
using System.IO;
using System.Security.Cryptography;
using Parquet.Meta;
using Parquet.Meta.Proto;

namespace Parquet.Test.Encryption;

internal static class TestCryptoUtils {
    public static ThriftCompactProtocolReader R(byte[] buf)
        => new ThriftCompactProtocolReader(new MemoryStream(buf));

    public static ThriftCompactProtocolReader R(Stream s)
        => new ThriftCompactProtocolReader(s);

    public static byte[] Le16(short v) => BitConverter.GetBytes(v);

    public static byte[] FrameGcm(byte[] nonce12, byte[] ciphertext, byte[] tag16) {
        int len = nonce12.Length + ciphertext.Length + tag16.Length;
        using var ms = new MemoryStream();
        ms.Write(BitConverter.GetBytes(len), 0, 4);
        ms.Write(nonce12, 0, nonce12.Length);
        ms.Write(ciphertext, 0, ciphertext.Length);
        ms.Write(tag16, 0, tag16.Length);
        return ms.ToArray();
    }

    public static byte[] FrameCtr(byte[] nonce12, byte[] ciphertext) {
        int len = nonce12.Length + ciphertext.Length;
        using var ms = new MemoryStream();
        ms.Write(BitConverter.GetBytes(len), 0, 4);
        ms.Write(nonce12, 0, nonce12.Length);
        ms.Write(ciphertext, 0, ciphertext.Length);
        return ms.ToArray();
    }

    public static AesGcm NewAesGcm(byte[] key) {
#if NET8_0_OR_GREATER
        return new AesGcm(key, 16);
#else
        return new AesGcm(key);
#endif
    }

    public static byte[] BuildAad(byte[] prefix, byte[] fileUnique, ParquetModules module, short? rg = null, short? col = null, short? page = null) {
        using var ms = new MemoryStream();
        ms.Write(prefix, 0, prefix.Length);
        ms.Write(fileUnique, 0, fileUnique.Length);
        ms.WriteByte((byte)module);
        if(rg.HasValue)
            ms.Write(Le16(rg.Value), 0, 2);
        if(col.HasValue)
            ms.Write(Le16(col.Value), 0, 2);
        if(page.HasValue)
            ms.Write(Le16(page.Value), 0, 2);
        return ms.ToArray();
    }

    public static byte[] XorCtr(ICryptoTransform ecbEncryptor, byte[] iv16, byte[] input) {
        byte[] counter = (byte[])iv16.Clone();
        byte[] output = new byte[input.Length];
        int i = 0;
        while(i < input.Length) {
            byte[] ks = new byte[16];
            ecbEncryptor.TransformBlock(counter, 0, 16, ks, 0);

            int n = Math.Min(16, input.Length - i);
            for(int j = 0; j < n; j++)
                output[i + j] = (byte)(input[i + j] ^ ks[j]);

            for(int p = 15; p >= 12; p--)  // big-endian increment
                if(++counter[p] != 0)
                    break;

            i += n;
        }
        return output;
    }
}
