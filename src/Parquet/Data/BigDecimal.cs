using System;
using System.Linq;
using System.Numerics;
using Parquet.Meta;

namespace Parquet.Data;

/// <summary>
/// A class that encapsulates decimal that goes out of range of .NET's <see cref="System.Decimal"/>.
/// It does not implement any math and should only be treated as a container for a very large decimal value. Big decimal math is out of scope of this project.
/// See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal.
/// </summary>
public struct BigDecimal {

    /// <summary>
    /// Raw value read as is
    /// </summary>
    public BigInteger UnscaledValue { get; }

    /// <summary>
    /// The precision of the decimal value (the total number of significant digits in the number, both before and after the decimal point)
    /// </summary>
    public int Precision { get; }

    /// <summary>
    /// The scale of the decimal value (the number of digits to the right of the decimal point i.e. the fractional part)
    /// </summary>
    public int Scale { get; }

    /// <summary>
    /// Construct big decimal
    /// </summary>
    /// <param name="unscaledValue"></param>
    /// <param name="precision"></param>
    /// <param name="scale"></param>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public BigDecimal(BigInteger unscaledValue, int precision, int scale) {
        UnscaledValue = unscaledValue;
        if(scale > precision)
            throw new ArgumentOutOfRangeException(nameof(scale), $"scale ({scale}) cannot be larger than precision ({precision}).");
        Precision = precision;
        Scale = scale;
    }

    internal BigDecimal(byte[] data, SchemaElement schema, bool isReversed = false) {
        if(!isReversed)
            data = Enumerable.Reverse(data).ToArray();

        UnscaledValue = new BigInteger(data);
        Precision = schema.Precision!.Value;
        Scale = schema.Scale ?? 0;
    }

    /// <summary>
    /// Creates a new instance of <see cref="BigDecimal"/> from a <see cref="System.Decimal"/>.
    /// </summary>
    public static BigDecimal FromDecimal(decimal d, int? precision, int? scale) {
        if(precision == null)
            throw new ArgumentNullException(nameof(precision), "precision is required");
        if(scale == null)
            scale = 0;

        BigInteger scaleMultiplier = BigInteger.Pow(10, scale.Value);
        BigInteger bscaled = new BigInteger(d);
        decimal scaled = d - (decimal)bscaled;
        decimal unscaled = scaled * (decimal)scaleMultiplier;
        BigInteger unscaledBig = (bscaled * scaleMultiplier) + new BigInteger(unscaled);
        return new BigDecimal(unscaledBig, precision.Value, scale.Value);
    }

    internal static decimal ToSystemDecimal(byte[] data, SchemaElement tse) {
        data = Enumerable.Reverse(data).ToArray();
        BigInteger scaleMultiplier = BigInteger.Pow(10, tse.Scale ?? 0);
        var unscaled = new BigInteger(data);
        decimal ipScaled = (decimal)BigInteger.DivRem(unscaled, scaleMultiplier, out BigInteger fpUnscaled);
        decimal fpScaled = (decimal)fpUnscaled / (decimal)scaleMultiplier;

        return ipScaled + fpScaled;
    }

    /// <summary>
    /// Gets buffer size enough to be able to hold the decimal number of a specific precision
    /// </summary>
    /// <param name="precision">Precision value</param>
    /// <returns>Length in bytes</returns>
    public static int GetBufferSize(int precision) {
        //according to impala source: http://impala.io/doc/html/parquet-common_8h_source.html

        int size;

        switch(precision) {
            case 1:
            case 2:
                size = 1;
                break;
            case 3:
            case 4:
                size = 2;
                break;
            case 5:
            case 6:
                size = 3;
                break;
            case 7:
            case 8:
            case 9:
                size = 4;
                break;
            case 10:
            case 11:
                size = 5;
                break;
            case 12:
            case 13:
            case 14:
                size = 6;
                break;
            case 15:
            case 16:
                size = 7;
                break;
            case 17:
            case 18:
                size = 8;
                break;
            case 19:
            case 20:
            case 21:
                size = 9;
                break;
            case 22:
            case 23:
                size = 10;
                break;
            case 24:
            case 25:
            case 26:
                size = 11;
                break;
            case 27:
            case 28:
                size = 12;
                break;
            case 29:
            case 30:
            case 31:
                size = 13;
                break;
            case 32:
            case 33:
                size = 14;
                break;
            case 34:
            case 35:
                size = 15;
                break;
            case 36:
            case 37:
            case 38:
                size = 16;
                break;
            default:
                size = 16;
                break;
        }

        return size;
    }

    private byte[] AllocateResult() {
        int size = GetBufferSize(Precision);
        return new byte[size];
    }

    internal byte[] GetBytes() {
        byte[] result = AllocateResult();
        WriteBytes(result.AsSpan());
        return result;
    }

    internal int WriteBytes(Span<byte> destination) {
        int size = GetBufferSize(Precision);
        if(destination.Length < size)
            throw new ArgumentException($"destination buffer is {destination.Length} but need {size} bytes");

        Span<byte> result = destination.Slice(0, size);

        // write little-endian bytes directly into the span
        if(!UnscaledValue.TryWriteBytes(result, out int bytesWritten, isUnsigned: false, isBigEndian: false))
            throw new NotSupportedException($"decimal data buffer does not fit into {size} bytes");

        if(bytesWritten > size)
            throw new NotSupportedException($"decimal data buffer is {bytesWritten} but result must fit into {size} bytes");

        // pad with sign extension
        if(UnscaledValue.Sign == -1) {
            result.Slice(bytesWritten).Fill(0xFF);
        } else {
            result.Slice(bytesWritten).Fill(0);
        }

        // reverse to big-endian
        result.Reverse();
        return size;
    }

    /// <summary>
    /// String representation
    /// </summary>
    public override string ToString() {
        // When scale is zero, this is a plain integer value.
        if(Scale == 0)
            return UnscaledValue.ToString();

        // Use absolute value for splitting into integer and fractional parts to avoid
        // negative remainders, which would otherwise break zero-padding logic.
        BigInteger scaleMultiplier = BigInteger.Pow(10, Scale);
        BigInteger sign = UnscaledValue.Sign;
        BigInteger absUnscaled = BigInteger.Abs(UnscaledValue);

        BigInteger ipAbs = BigInteger.DivRem(absUnscaled, scaleMultiplier, out BigInteger fpAbs);
        string fpStr = fpAbs.ToString().PadLeft(Scale, '0');

        string result = $"{ipAbs}.{fpStr}";

        // Apply the sign to the combined value. This ensures exactly one leading minus
        // sign for negative numbers (including cases like -0.00123).
        if(sign < 0 && (ipAbs != 0 || fpAbs != 0)) {
            result = "-" + result;
        }

        return result;
    }
}