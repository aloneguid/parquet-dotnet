using System;
using System.Collections.Generic;
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
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

        int[] des = new int[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeInt32_Max() {

        int[] input = new[] { int.MaxValue, int.MaxValue - 5, int.MaxValue - 3, int.MaxValue - 1, int.MaxValue - 3, int.MaxValue - 4, };

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

        int[] des = new int[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeInt32_Min() {

        int[] input = new[] { int.MinValue, int.MinValue + 5, int.MinValue + 3, int.MinValue + 1, int.MinValue + 3, int.MinValue + 4, };

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

        int[] des = new int[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }
    [Fact]
    public void EncodeAndDecodeInt32_1_100000() {
        int total = 100000;
        int[] input = Enumerable.Range(1, total).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

        int[] des = new int[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeInt64() {
        long[] input = new[] { 7L, 5L, 3L, 1L, -2L, 3L, 4L, -5L, };

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

        long[] des = new long[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeInt64_Max() {

        long[] input = new[] { long.MaxValue, long.MaxValue - 5, long.MaxValue - 3, long.MaxValue - 1, long.MaxValue - 3, long.MaxValue - 4, };

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

        long[] des = new long[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Fact]
    public void EncodeAndDecodeInt64_Min() {

        long[] input = new[] { long.MinValue, long.MinValue + 5, long.MinValue + 3, long.MinValue + 1, long.MinValue + 3, long.MinValue + 4, };

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

        long[] des = new long[input.Length];
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }


    [Fact]
    public void EncodeAndDecodeInt64_1_100000() {
        int total = 100000;
        long[] input = Enumerable.Range(1, total).Select(i => (long)i).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

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
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

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
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

        long[] des = new long[input.Length];
        long i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    #region Integer Compatible Types Tests


    public static IEnumerable<object[]> NewIntegerTypeBasicTestData => new List<object[]> {
        new object[] { new short[] { 7, 5, 3, 1, -2, 3, 4, -5 } },
        new object[] { new ushort[] { 7, 5, 3, 1, 2, 3, 4, 5 } },
        new object[] { new uint[] { 7u, 5u, 3u, 1u, 2u, 3u, 4u, 5u } },
        new object[] { new ulong[] { 7ul, 5ul, 3ul, 1ul, 2ul, 3ul, 4ul, 5ul } }
    };

    [Theory]
    [MemberData(nameof(NewIntegerTypeBasicTestData))]
    public void EncodeAndDecodeNewIntegerTypes(Array input) {
        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

        Array des = Array.CreateInstance(input.GetType().GetElementType()!, input.Length);
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    #endregion

    #region Edge Case Tests


    public static IEnumerable<object[]> NewIntegerTypeEdgeCaseTestData => new List<object[]> {
        new object[] { new short[] { short.MinValue, short.MaxValue, short.MinValue, short.MaxValue } },
        new object[] { new ushort[] { ushort.MinValue, ushort.MaxValue, ushort.MinValue, ushort.MaxValue } },
        new object[] { new uint[] { uint.MinValue, uint.MaxValue, uint.MinValue, uint.MaxValue } },
        new object[] { new ulong[] { 0ul, (ulong)long.MaxValue, 100ul, 1000ul } }
    };

    [Theory]
    [MemberData(nameof(NewIntegerTypeEdgeCaseTestData))]
    public void EncodeAndDecodeNewIntegerTypes_EdgeCases(Array input) {
        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

        Array des = Array.CreateInstance(input.GetType().GetElementType()!, input.Length);
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

        Assert.Equal(input, des);
    }

    [Theory]
    [InlineData(typeof(short))]
    [InlineData(typeof(ushort))]
    [InlineData(typeof(int))]
    [InlineData(typeof(uint))]
    [InlineData(typeof(long))]
    [InlineData(typeof(ulong))]
    public void EncodeAndDecodeEmptyArray(Type type) {
        Array emptyInput = Array.CreateInstance(type, 0);

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode(emptyInput, 0, 0, ms);

        Array des = Array.CreateInstance(type, 0);
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 0, out int b);

        Assert.Empty(des);
    }

    [Theory]
    [InlineData(typeof(short))]
    [InlineData(typeof(ushort))]
    [InlineData(typeof(int))]
    [InlineData(typeof(uint))]
    [InlineData(typeof(long))]
    [InlineData(typeof(ulong))]
    public void EncodeAndDecodeSingleElement(Type type) {
        Array singleInput = Array.CreateInstance(type, 1);

        if(type == typeof(short))
            ((short[])singleInput)[0] = -1000;
        else if(type == typeof(ushort))
            ((ushort[])singleInput)[0] = 1000;
        else if(type == typeof(int))
            ((int[])singleInput)[0] = -100000;
        else if(type == typeof(uint))
            ((uint[])singleInput)[0] = 100000u;
        else if(type == typeof(long))
            ((long[])singleInput)[0] = -1000000L;
        else if(type == typeof(ulong))
            ((ulong[])singleInput)[0] = 1000000ul;

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode(singleInput, 0, 1, ms);

        Array des = Array.CreateInstance(type, 1);
        int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, 1, out int b);

        Assert.Equal(singleInput.GetValue(0), des.GetValue(0));
    }

    #endregion

    #region ULong Fallback Tests

    [Fact]
    public void EncodeUInt64_ExceedsInt64Max_ShouldThrowNotSupportedException() {
        ulong[] input = new ulong[] { ulong.MaxValue, (ulong)long.MaxValue + 1, ulong.MaxValue - 1 };

        using var ms = new MemoryStream();

        Assert.Throws<NotSupportedException>(() =>
            DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms));
    }

    public static IEnumerable<object[]> UInt64CanEncodeTestData => new List<object[]> {
        new object[] { new ulong[] { ulong.MaxValue }, false, "ExceedsInt64Max" },
        new object[] { new ulong[] { 0ul, (ulong)long.MaxValue, 100ul }, true, "WithinInt64Range" },
        new object[] { new ulong[] { 0ul, (ulong)long.MaxValue, ulong.MaxValue }, false, "MixedRange" },
        new object[] { new ulong[0], true, "EmptyArray" },
        new object[] { new ulong[] { (ulong)long.MaxValue + 1 }, false, "SingleElementExceedsMax" }
    };

    [Theory]
    [MemberData(nameof(UInt64CanEncodeTestData))]
    public void CanEncode_UInt64_VariousScenarios(ulong[] input, bool expectedResult, string scenario) {
        bool canEncode = DeltaBinaryPackedEncoder.CanEncode(input, 0, input.Length);

        Assert.Equal(expectedResult, canEncode);
    }

    #endregion

    #region Performance Benchmark Tests


    [Theory]
    [InlineData(10000)]
    [InlineData(100000)]
    public void PerformanceTest_UInt16_Sequential(int count) {
        ushort[] input = Enumerable.Range(0, count).Select(i => (ushort)(i % 65536)).ToArray();

        using var ms = new MemoryStream();
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

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
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

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
        DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

        ulong[] des = new ulong[input.Length];
        int decoded = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int _);

        Assert.Equal(input, des);
        Assert.Equal(input.Length, decoded);
    }

    [Fact]
    public void CompressionRatio_TestAllTypes() {
        int count = 10000;

        // Test sequential data compression for each type
        Type[] types = new[] {
            typeof(short), typeof(ushort),
            typeof(int), typeof(uint), typeof(long), typeof(ulong)
        };

        foreach(Type type in types) {
            Array input = CreateSequentialArray(type, count);

            using var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

            int originalSize = GetTypeSize(type) * count;
            int encodedSize = (int)ms.Length;

            Assert.True(encodedSize < originalSize, $"Type {type.Name}: encoded size ({encodedSize}) should be less than original size ({originalSize})");

        }
    }

    private static Array CreateSequentialArray(Type type, int count) {
        Array array = Array.CreateInstance(type, count);

        if(type == typeof(short)) {
            for(int i = 0; i < count; i++)
                ((short[])array)[i] = (short)(i % 32768);
        } else if(type == typeof(ushort)) {
            for(int i = 0; i < count; i++)
                ((ushort[])array)[i] = (ushort)(i % 65536);
        } else if(type == typeof(int)) {
            for(int i = 0; i < count; i++)
                ((int[])array)[i] = i;
        } else if(type == typeof(uint)) {
            for(int i = 0; i < count; i++)
                ((uint[])array)[i] = (uint)i;
        } else if(type == typeof(long)) {
            for(int i = 0; i < count; i++)
                ((long[])array)[i] = i;
        } else if(type == typeof(ulong)) {
            for(int i = 0; i < count; i++)
                ((ulong[])array)[i] = (ulong)i;
        }

        return array;
    }

    private static int GetTypeSize(Type type) {
        if(type == typeof(short) || type == typeof(ushort))
            return 2;
        if(type == typeof(int) || type == typeof(uint))
            return 4;
        if(type == typeof(long) || type == typeof(ulong))
            return 8;
        throw new ArgumentException($"Unsupported type: {type}");
    }

    #endregion
}
