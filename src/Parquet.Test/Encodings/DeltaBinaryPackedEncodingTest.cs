using System;
using System.IO;
using System.Linq;
using Parquet.Encodings;
using Xunit;

namespace Parquet.Test.Encodings; 

public class DeltaBinaryPackedEncodingTest : TestBase {

    #region Type Support Tests

    [Theory]
    [InlineData(typeof(short))]
    [InlineData(typeof(ushort))]
    [InlineData(typeof(int))]
    [InlineData(typeof(uint))]
    [InlineData(typeof(long))]
    [InlineData(typeof(ulong))]
    public void IsSupported_AllIntegerTypes_ReturnsTrue(Type type) {
        // Act
        bool isSupported = DeltaBinaryPackedEncoder.IsSupported(type);

        // Assert
        Assert.True(isSupported, $"Type {type.Name} should be supported by delta encoding");
    }

    [Theory]
    [InlineData(typeof(byte))]
    [InlineData(typeof(sbyte))]
    [InlineData(typeof(float))]
    [InlineData(typeof(double))]
    [InlineData(typeof(decimal))]
    [InlineData(typeof(string))]
    [InlineData(typeof(DateTime))]
    [InlineData(typeof(bool))]
    public void IsSupported_NonIntegerTypes_ReturnsFalse(Type type) {
        // Act
        bool isSupported = DeltaBinaryPackedEncoder.IsSupported(type);

        // Assert
        Assert.False(isSupported, $"Type {type.Name} should not be supported by delta encoding");
    }

    #endregion

    [Fact]
    public void EncodeAndDecodeInt32() {

        int[] input = new[] { 7, 5, 3, 1, -2, 3, 4, -5, };

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<int>(input, ms);

        int[] des = new int[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeInt32_Max() {

        int[] input = new[] { int.MaxValue, int.MaxValue - 5, int.MaxValue - 3, int.MaxValue - 1, int.MaxValue - 3, int.MaxValue - 4, };

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<int>(input, ms);

        int[] des = new int[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeInt32_Min() {

        int[] input = new[] { int.MinValue, int.MinValue + 5, int.MinValue + 3, int.MinValue + 1, int.MinValue + 3, int.MinValue + 4, };

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<int>(input, ms);

        int[] des = new int[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }
    [Fact]
    public void EncodeAndDecodeInt32_1_100000() {
        int total = 100000;
        int[] input = Enumerable.Range(1, total).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<int>(input, ms);

        int[] des = new int[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeInt64() {
        long[] input = new[] { 7L, 5L, 3L, 1L, -2L, 3L, 4L, -5L, };

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<long>(input, ms);

        long[] des = new long[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeInt64_Max() {

        long[] input = new[] { long.MaxValue, long.MaxValue - 5, long.MaxValue - 3, long.MaxValue - 1, long.MaxValue - 3, long.MaxValue - 4, };

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<long>(input, ms);

        long[] des = new long[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeInt64_Min() {

        long[] input = new[] { long.MinValue, long.MinValue + 5, long.MinValue + 3, long.MinValue + 1, long.MinValue + 3, long.MinValue + 4, };

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<long>(input, ms);

        long[] des = new long[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }


    [Fact]
    public void EncodeAndDecodeInt64_1_100000() {
        int total = 100000;
        long[] input = Enumerable.Range(1, total).Select(i => (long)i).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<long>(input, ms);

        long[] des = new long[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(128)]
    [InlineData(129)]
    [InlineData(1023)]
    [InlineData(1024)]
    [InlineData(1025)]
    [InlineData(4095)]
    [InlineData(4096)]
    [InlineData(4097)]
    [InlineData((1024 * 9) - 1)]
    [InlineData(1024 * 9)]
    [InlineData((1024 * 9) + 1)]
    [InlineData(10000)]
    [InlineData(102541)]
    [InlineData(3402541)]
    public void EncodeAndDecodeInt32_Random_Overflow(int total) {
        var r = new Random(0);
        int[] input = Enumerable.Range(0, total).Select(i => r.Next(int.MinValue, int.MaxValue)).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<int>(input, ms);

        int[] des = new int[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(128)]
    [InlineData(129)]
    [InlineData(1023)]
    [InlineData(1024)]
    [InlineData(1025)]
    [InlineData(4095)]
    [InlineData(4096)]
    [InlineData(4097)]
    [InlineData((1024 * 9) - 1)]
    [InlineData(1024 * 9)]
    [InlineData((1024 * 9) + 1)]
    [InlineData(10000)]
    [InlineData(102541)]
    [InlineData(3402541)]
    public void EncodeAndDecodeInt64_Random_Overflow(int total) {
        var r = new Random(0);

        long[] input = Enumerable.Range(0, total).Select(i => {
            byte[] buffer = new byte[8];
            r.NextBytes(buffer);
            long randomInt64 = BitConverter.ToInt64(buffer, 0);
            return randomInt64;
        }).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<long>(input, ms);

        long[] des = new long[input.Length];
        long i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    #region Integer Compatible Types Tests

    [Fact]
    public void EncodeAndDecodeInt16() {
        short[] input = [7, 5, 3, 1, -2, 3, 4, -5];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<short>(input, ms);

        short[] des = new short[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeUInt16() {
        ushort[] input = [7, 5, 3, 1, 2, 3, 4, 5];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<ushort>(input, ms);

        ushort[] des = new ushort[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeUInt32() {
        uint[] input = [7u, 5u, 3u, 1u, 2u, 3u, 4u, 5u];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<uint>(input, ms);

        uint[] des = new uint[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeUInt64() {
        ulong[] input = [7ul, 5ul, 3ul, 1ul, 2ul, 3ul, 4ul, 5ul];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<ulong>(input, ms);

        ulong[] des = new ulong[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    #endregion

    #region Edge Case Tests

    [Fact]
    public void EncodeAndDecodeInt16_EdgeCases() {
        short[] input = [short.MinValue, short.MaxValue, short.MinValue, short.MaxValue];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<short>(input, ms);

        short[] des = new short[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeUInt16_EdgeCases() {
        ushort[] input = [ushort.MinValue, ushort.MaxValue, ushort.MinValue, ushort.MaxValue];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<ushort>(input, ms);

        ushort[] des = new ushort[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeUInt32_EdgeCases() {
        uint[] input = [uint.MinValue, uint.MaxValue, uint.MinValue, uint.MaxValue];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<uint>(input, ms);

        uint[] des = new uint[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeUInt64_EdgeCases() {
        ulong[] input = [0ul, (ulong)long.MaxValue, 100ul, 1000ul];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<ulong>(input, ms);

        ulong[] des = new ulong[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeInt16_Empty() {
        short[] input = [];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<short>(input, ms);

        short[] des = [];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 0, out int b);

        Assert.Empty(des);
    }

    [Fact]
    public void EncodeAndDecodeUInt16_Empty() {
        ushort[] input = [];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<ushort>(input, ms);

        ushort[] des = [];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 0, out int b);

        Assert.Empty(des);
    }

    [Fact]
    public void EncodeAndDecodeInt32_Empty() {
        int[] input = [];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<int>(input, ms);

        int[] des = [];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 0, out int b);

        Assert.Empty(des);
    }

    [Fact]
    public void EncodeAndDecodeUInt32_Empty() {
        uint[] input = [];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<uint>(input, ms);

        uint[] des = [];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 0, out int b);

        Assert.Empty(des);
    }

    [Fact]
    public void EncodeAndDecodeInt64_Empty() {
        long[] input = [];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<long>(input, ms);

        long[] des = [];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 0, out int b);

        Assert.Empty(des);
    }

    [Fact]
    public void EncodeAndDecodeUInt64_Empty() {
        ulong[] input = [];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<ulong>(input, ms);

        ulong[] des = [];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 0, out int b);

        Assert.Empty(des);
    }

    [Fact]
    public void EncodeAndDecodeInt16_SingleElement() {
        short[] input = [-1000];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<short>(input, ms);

        short[] des = new short[1];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 1, out int b);

        Assert.Equal(input[0], des[0]);
    }

    [Fact]
    public void EncodeAndDecodeUInt16_SingleElement() {
        ushort[] input = [1000];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<ushort>(input, ms);

        ushort[] des = new ushort[1];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 1, out int b);

        Assert.Equal(input[0], des[0]);
    }

    [Fact]
    public void EncodeAndDecodeInt32_SingleElement() {
        int[] input = [-100000];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<int>(input, ms);

        int[] des = new int[1];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 1, out int b);

        Assert.Equal(input[0], des[0]);
    }

    [Fact]
    public void EncodeAndDecodeUInt32_SingleElement() {
        uint[] input = [100000u];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<uint>(input, ms);

        uint[] des = new uint[1];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 1, out int b);

        Assert.Equal(input[0], des[0]);
    }

    [Fact]
    public void EncodeAndDecodeInt64_SingleElement() {
        long[] input = [-1000000L];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<long>(input, ms);

        long[] des = new long[1];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 1, out int b);

        Assert.Equal(input[0], des[0]);
    }

    [Fact]
    public void EncodeAndDecodeUInt64_SingleElement() {
        ulong[] input = [1000000ul];

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<ulong>(input, ms);

        ulong[] des = new ulong[1];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 1, out int b);

        Assert.Equal(input[0], des[0]);
    }

    #endregion

    #region ULong Fallback Tests

    [Fact]
    public void EncodeUInt64_ExceedsInt64Max_ShouldThrowNotSupportedException() {
        ulong[] input = [ulong.MaxValue, (ulong)long.MaxValue + 1, ulong.MaxValue - 1];

        using var ms = new MemoryStream();

        Assert.Throws<NotSupportedException>(() =>
            DeltaBinaryPackedEncoder.Encode<ulong>(input, ms));
    }

    [Fact]
    public void CanEncode_UInt64_ExceedsInt64Max_ReturnsFalse() {
        ulong[] input = [ulong.MaxValue];
        Assert.False(DeltaBinaryPackedEncoder.CanEncode<ulong>(input));
    }

    [Fact]
    public void CanEncode_UInt64_WithinInt64Range_ReturnsTrue() {
        ulong[] input = [0ul, (ulong)long.MaxValue, 100ul];
        Assert.True(DeltaBinaryPackedEncoder.CanEncode<ulong>(input));
    }

    [Fact]
    public void CanEncode_UInt64_MixedRange_ReturnsFalse() {
        ulong[] input = [0ul, (ulong)long.MaxValue, ulong.MaxValue];
        Assert.False(DeltaBinaryPackedEncoder.CanEncode<ulong>(input));
    }

    [Fact]
    public void CanEncode_UInt64_EmptySpan_ReturnsFalse() {
        ReadOnlySpan<ulong> input = [];
        Assert.False(DeltaBinaryPackedEncoder.CanEncode<ulong>(input));
    }

    [Fact]
    public void CanEncode_UInt64_SingleElementExceedsMax_ReturnsFalse() {
        ulong[] input = [(ulong)long.MaxValue + 1];
        Assert.False(DeltaBinaryPackedEncoder.CanEncode<ulong>(input));
    }

    #endregion

    #region Performance Benchmark Tests


    [Theory]
    [InlineData(10000)]
    [InlineData(100000)]
    public void PerformanceTest_UInt16_Sequential(int count) {
        ushort[] input = Enumerable.Range(0, count).Select(i => (ushort)(i % 65536)).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<ushort>(input, ms);

        ushort[] des = new ushort[input.Length];
        int decoded = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int _);

        Assert.Equal(input, des);
        Assert.Equal(input.Length, decoded);
    }

    [Theory]
    [InlineData(10000)]
    [InlineData(100000)]
    public void PerformanceTest_UInt32_Sequential(int count) {
        uint[] input = Enumerable.Range(0, count).Select(i => (uint)i).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<uint>(input, ms);

        uint[] des = new uint[input.Length];
        int decoded = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int _);

        Assert.Equal(input, des);
        Assert.Equal(input.Length, decoded);
    }

    [Theory]
    [InlineData(10000)]
    [InlineData(100000)]
    public void PerformanceTest_UInt64_Sequential(int count) {
        ulong[] input = Enumerable.Range(0, count).Select(i => (ulong)i).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<ulong>(input, ms);

        ulong[] des = new ulong[input.Length];
        int decoded = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int _);

        Assert.Equal(input, des);
        Assert.Equal(input.Length, decoded);
    }

    [Fact]
    public void CompressionRatio_Int16_Sequential() {
        int count = 10000;
        short[] input = Enumerable.Range(0, count).Select(i => (short)(i % 32768)).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<short>(input, ms);

        int originalSize = sizeof(short) * count;
        int encodedSize = (int)ms.Length;

        Assert.True(encodedSize < originalSize, $"Encoded size ({encodedSize}) should be less than original size ({originalSize})");
    }

    [Fact]
    public void CompressionRatio_UInt16_Sequential() {
        int count = 10000;
        ushort[] input = Enumerable.Range(0, count).Select(i => (ushort)(i % 65536)).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<ushort>(input, ms);

        int originalSize = sizeof(ushort) * count;
        int encodedSize = (int)ms.Length;

        Assert.True(encodedSize < originalSize, $"Encoded size ({encodedSize}) should be less than original size ({originalSize})");
    }

    [Fact]
    public void CompressionRatio_Int32_Sequential() {
        int count = 10000;
        int[] input = Enumerable.Range(0, count).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<int>(input, ms);

        int originalSize = sizeof(int) * count;
        int encodedSize = (int)ms.Length;

        Assert.True(encodedSize < originalSize, $"Encoded size ({encodedSize}) should be less than original size ({originalSize})");
    }

    [Fact]
    public void CompressionRatio_UInt32_Sequential() {
        int count = 10000;
        uint[] input = Enumerable.Range(0, count).Select(i => (uint)i).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<uint>(input, ms);

        int originalSize = sizeof(uint) * count;
        int encodedSize = (int)ms.Length;

        Assert.True(encodedSize < originalSize, $"Encoded size ({encodedSize}) should be less than original size ({originalSize})");
    }

    [Fact]
    public void CompressionRatio_Int64_Sequential() {
        int count = 10000;
        long[] input = Enumerable.Range(0, count).Select(i => (long)i).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<long>(input, ms);

        int originalSize = sizeof(long) * count;
        int encodedSize = (int)ms.Length;

        Assert.True(encodedSize < originalSize, $"Encoded size ({encodedSize}) should be less than original size ({originalSize})");
    }

    [Fact]
    public void CompressionRatio_UInt64_Sequential() {
        int count = 10000;
        ulong[] input = Enumerable.Range(0, count).Select(i => (ulong)i).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode<ulong>(input, ms);

        int originalSize = sizeof(ulong) * count;
        int encodedSize = (int)ms.Length;

        Assert.True(encodedSize < originalSize, $"Encoded size ({encodedSize}) should be less than original size ({originalSize})");
    }

    #endregion
}
