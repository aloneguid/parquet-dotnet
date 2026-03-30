using System;
using System.IO;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.File.Values.Primitives;
using Parquet.Schema;
using Parquet.Test.Util;
using Xunit;

namespace Parquet.Test.Types;

public class EndToEndTypeTest : TestBase {

    /// <summary>
    /// Writes a single non-nullable struct value using generic WriteAsync and reads it back.
    /// </summary>
    private static async Task<T> WriteReadSingleAsync<T>(DataField field, T value, ParquetOptions? options = null) where T : struct {
        byte[] data;
        options ??= new ParquetOptions();

        using(var ms = new MemoryStream()) {
            await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), ms, options)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteAsync<T>(field, new T[] { value }.AsMemory());
            }
            data = ms.ToArray();
        }

        using(var ms = new MemoryStream(data)) {
            ms.Position = 0;
            await using ParquetReader reader = await ParquetReader.CreateAsync(ms, options);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
            DataColumn column = await rowGroupReader.ReadColumnAsync(field);
            return (T)column.Data.GetValue(0)!;
        }
    }

    /// <summary>
    /// Writes a single nullable struct value using generic WriteAsync and reads it back.
    /// </summary>
    private static async Task<T?> WriteReadSingleNullableAsync<T>(DataField field, T? value = null, ParquetOptions? options = null) where T : struct {
        byte[] data;
        options ??= new ParquetOptions();

        using(var ms = new MemoryStream()) {
            await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), ms, options)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteAsync<T>(field, new T?[] { value }.AsMemory());
            }
            data = ms.ToArray();
        }

        using(var ms = new MemoryStream(data)) {
            ms.Position = 0;
            await using ParquetReader reader = await ParquetReader.CreateAsync(ms, options);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
            DataColumn column = await rowGroupReader.ReadColumnAsync(field);
            object? raw = column.Data.GetValue(0);
            return raw is T t ? t : null;
        }
    }

    /// <summary>
    /// Writes a single string value using the string-specific WriteAsync and reads it back.
    /// </summary>
    private static async Task<string?> WriteReadSingleStringAsync(DataField field, string value) {
        byte[] data;

        using(var ms = new MemoryStream()) {
            await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), ms)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteAsync(field, [value]);
            }
            data = ms.ToArray();
        }

        using(var ms = new MemoryStream(data)) {
            ms.Position = 0;
            await using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
            DataColumn column = await rowGroupReader.ReadColumnAsync(field);
            return (string?)column.Data.GetValue(0);
        }
    }

    /// <summary>
    /// Writes a single byte array value using WriteColumnAsync and reads it back.
    /// Binary columns are not supported by the generic WriteAsync overloads.
    /// </summary>
    private static async Task<byte[]?> WriteReadSingleByteArrayAsync(DataField field, byte[] value) {
        byte[] data;

        using(var ms = new MemoryStream()) {
            await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), ms)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteAsync(field, [value]);
            }
            data = ms.ToArray();
        }

        using(var ms = new MemoryStream(data)) {
            ms.Position = 0;
            await using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
            DataColumn column = await rowGroupReader.ReadColumnAsync(field);
            return (byte[]?)column.Data.GetValue(0);
        }
    }

    // --- String tests ---

    [Fact]
    public async Task Type_plain_string_writes_and_reads() {
        var field = new DataField<string>("string");
        string? actual = await WriteReadSingleStringAsync(field, "plain string");
        Assert.Equal("plain string", actual);
    }

    [Fact]
    public async Task Type_unicode_string_writes_and_reads() {
        var field = new DataField<string>("unicode string");
        string? actual = await WriteReadSingleStringAsync(field, "L'Or\u00e9al Paris");
        Assert.Equal("L'Or\u00e9al Paris", actual);
    }

    // --- Byte array tests ---

    [Fact]
    public async Task Type_byte_array_writes_and_reads() {
        var field = new DataField<byte[]>("byte array");
        byte[] expected = Encoding.UTF8.GetBytes("raw byte string");
        byte[]? actual = await WriteReadSingleByteArrayAsync(field, expected);
        Assert.Equal(expected, actual);
    }

    // --- Numeric tests ---

    [Fact]
    public async Task Type_float_writes_and_reads() {
        var field = new DataField<float>("float");
        float actual = await WriteReadSingleAsync(field, 1.23f);
        Assert.Equal(1.23f, actual);
    }

    [Fact]
    public async Task Type_double_writes_and_reads() {
        var field = new DataField<double>("double");
        double actual = await WriteReadSingleAsync(field, 10.44D);
        Assert.Equal(10.44D, actual);
    }

    [Fact]
    public async Task Type_long_writes_and_reads() {
        var field = new DataField<long>("long");
        long actual = await WriteReadSingleAsync(field, 1234L);
        Assert.Equal(1234L, actual);
    }

    // --- Decimal tests ---

    [Fact]
    public async Task Type_simple_decimal_writes_and_reads() {
        var field = new DataField<decimal>("decDefault");
        decimal actual = await WriteReadSingleAsync(field, 123.4m);
        Assert.Equal(123.4m, actual);
    }

    [Fact]
    public async Task Type_huge_decimal_writes_and_reads() {
        var field = new DataField<decimal>("hugeDec");
        decimal actual = await WriteReadSingleAsync(field, 83086059037282.54m);
        Assert.Equal(83086059037282.54m, actual);
    }

    [Fact]
    public async Task Type_int32_decimal_writes_and_reads() {
        var field = new DecimalDataField("decInt32", 4, 1);
        decimal actual = await WriteReadSingleAsync(field, 12.4m);
        Assert.Equal(12.4m, actual);
    }

    [Fact]
    public async Task Type_int64_decimal_writes_and_reads() {
        var field = new DecimalDataField("decInt64", 17, 12);
        decimal actual = await WriteReadSingleAsync(field, 1234567.88m);
        Assert.Equal(1234567.88m, actual);
    }

    [Fact]
    public async Task Type_fixed_byte_array_decimal_writes_and_reads() {
        var field = new DecimalDataField("decFixedByteArray", 48, 12);
        decimal actual = await WriteReadSingleAsync(field, 34434.5m);
        Assert.Equal(34434.5m, actual);
    }

    [Fact]
    public async Task Type_negative_decimal_writes_and_reads() {
        var field = new DecimalDataField("decMinus", 10, 2, true);
        decimal actual = await WriteReadSingleAsync(field, -1m);
        Assert.Equal(-1m, actual);
    }

    [Fact]
    public async Task Type_scale_zero_decimal_writes_and_reads() {
        var field = new DecimalDataField("v", 10, 0, true);
        decimal actual = await WriteReadSingleAsync(field, 10.0m);
        Assert.Equal(10.0m, actual);
    }

    [Fact]
    public async Task Type_really_big_decimal_writes_and_reads() {
        var field = new DecimalDataField("decReallyBig", 38, 2, clrType: typeof(BigDecimal));
        var expected = new BigDecimal(BigInteger.Parse("12345678901234567890123456789012345678"), 38, 2);
        BigDecimal actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Type_really_big_bigdecimal_writes_and_reads() {
        var field = new BigDecimalDataField("decReallyBig", 38, 2);
        var expected = new BigDecimal(BigInteger.Parse("12345678901234567890123456789012345678"), 38, 2);
        BigDecimal actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(expected, actual);
    }

    // --- DateTime tests ---

    [Fact]
    public async Task Type_simple_datetime_writes_and_reads() {
        var field = new DataField<DateTime>("datetime");
        DateTime expected = DateTime.UtcNow.RoundToSecond();
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Type_datetime_writes_and_reads() {
        var field = new DataField<DateTime>("dateTime");
        DateTime expected = DateTime.UtcNow.RoundToSecond();
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Type_impala_date_writes_and_reads() {
        var field = new DateTimeDataField("dateImpala", DateTimeFormat.Impala);
        DateTime expected = DateTime.UtcNow.RoundToSecond();
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Type_date_and_time_millis_writes_and_reads() {
        var field = new DateTimeDataField("dateDateAndTime", DateTimeFormat.DateAndTime);
        DateTime expected = DateTime.UtcNow.RoundToMillisecond();
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
        Assert.Equal(expected, actual);
    }

#if NET7_0_OR_GREATER
    [Fact]
    public async Task Type_date_and_time_micros_writes_and_reads() {
        var field = new DateTimeDataField("dateDateAndTime", DateTimeFormat.DateAndTimeMicros);
        DateTime expected = DateTime.UtcNow.RoundToMicrosecond();
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
        Assert.Equal(expected, actual);
    }
#endif

    [Fact]
    public async Task Type_datetime_unknown_kind_writes_and_reads() {
        var field = new DataField<DateTime>("dateTime unknown kind");
        var expected = new DateTime(2020, 06, 10, 11, 12, 13);
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
        DateTime expectedUtc = DateTime.SpecifyKind(expected, DateTimeKind.Utc);
        Assert.Equal(expectedUtc, actual);
    }

    [Fact]
    public async Task Type_impala_date_unknown_kind_writes_and_reads() {
        var field = new DateTimeDataField("dateImpala unknown kind", DateTimeFormat.Impala);
        var expected = new DateTime(2020, 06, 10, 11, 12, 13);
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
        DateTime expectedUtc = DateTime.SpecifyKind(expected, DateTimeKind.Utc);
        Assert.Equal(expectedUtc, actual);
    }

    [Fact]
    public async Task Type_date_and_time_unknown_kind_writes_and_reads() {
        var field = new DateTimeDataField("dateDateAndTime unknown kind", DateTimeFormat.DateAndTime);
        var expected = new DateTime(2020, 06, 10, 11, 12, 13);
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
        DateTime expectedUtc = DateTime.SpecifyKind(expected, DateTimeKind.Utc);
        Assert.Equal(expectedUtc, actual);
    }

    [Fact]
    public async Task Type_datetime_local_kind_writes_and_reads() {
        var field = new DataField<DateTime>("dateTime unknown kind");
        var expected = new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local);
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
        Assert.Equal(expected.ToUniversalTime(), actual);
    }

    [Fact]
    public async Task Type_impala_date_local_kind_writes_and_reads() {
        var field = new DateTimeDataField("dateImpala unknown kind", DateTimeFormat.Impala);
        var expected = new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local);
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
        Assert.Equal(expected.ToUniversalTime(), actual);
    }

    [Fact]
    public async Task Type_date_and_time_local_kind_writes_and_reads() {
        var field = new DateTimeDataField("dateDateAndTime unknown kind", DateTimeFormat.DateAndTime);
        var expected = new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local);
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
        Assert.Equal(expected.ToUniversalTime(), actual);
    }

    [Fact]
    public async Task Type_timestamp_utc_kind_writes_and_reads() {
        var field = new DateTimeDataField("timestamp utc kind", DateTimeFormat.Timestamp, true);
        var expected = new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Utc);
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Type_timestamp_local_kind_writes_and_reads() {
        var field = new DateTimeDataField("timestamp local kind", DateTimeFormat.Timestamp, false);
        var expected = new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local);
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Local, actual.Kind);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Type_date_writes_and_reads() {
        var field = new DateTimeDataField("dateDate", DateTimeFormat.Date);
        DateTime expected = DateTime.UtcNow.RoundToDay();
        DateTime actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
        // RoundToDay produces Unspecified kind, treat as UTC
        DateTime expectedUtc = expected.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(expected, DateTimeKind.Utc)
            : expected.ToUniversalTime();
        Assert.Equal(expectedUtc, actual);
    }

#if !NETCOREAPP3_1
    [Fact]
    public async Task Type_dateonly_writes_and_reads() {
        var field = new DataField<DateOnly>("dateOnly");
        DateOnly expected = DateOnly.FromDateTime(DateTime.UtcNow);
        var options = new ParquetOptions { UseDateOnlyTypeForDates = true };
        DateOnly actual = await WriteReadSingleAsync(field, expected, options);
        Assert.Equal(expected, actual);
    }
#endif

    // --- Interval test ---

    [Fact]
    public async Task Type_interval_writes_and_reads() {
        var field = new DataField<Interval>("interval");
        var expected = new Interval(3, 2, 1);
        Interval actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(expected, actual);
    }

    // --- TimeSpan tests ---

    [Fact]
    public async Task Type_time_micros_writes_and_reads() {
        var field = new TimeSpanDataField("timeMicros", TimeSpanFormat.MicroSeconds);
        var expected = new TimeSpan(DateTime.UtcNow.TimeOfDay.Ticks / 10 * 10);
        TimeSpan actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Type_time_millis_writes_and_reads() {
        var field = new TimeSpanDataField("timeMillis", TimeSpanFormat.MilliSeconds);
        var expected = new TimeSpan(DateTime.UtcNow.TimeOfDay.Ticks / 10000 * 10000);
        TimeSpan actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(expected, actual);
    }

#if NET6_0_OR_GREATER
    [Fact]
    public async Task Type_timeonly_micros_writes_and_reads() {
        var field = new TimeOnlyDataField("timeMicros", TimeSpanFormat.MicroSeconds);
        var expected = new TimeOnly(DateTime.UtcNow.TimeOfDay.Ticks / 10 * 10);
        TimeOnly actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Type_timeonly_millis_writes_and_reads() {
        var field = new TimeOnlyDataField("timeMillis", TimeSpanFormat.MilliSeconds);
        var expected = new TimeOnly(DateTime.UtcNow.TimeOfDay.Ticks / 10000 * 10000);
        TimeOnly actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(expected, actual);
    }
#endif

    // --- Integer boundary tests ---

    [Fact]
    public async Task Type_byte_min_value_writes_and_reads() {
        var field = new DataField<byte>("byte");
        byte actual = await WriteReadSingleAsync(field, byte.MinValue);
        Assert.Equal(byte.MinValue, actual);
    }

    [Fact]
    public async Task Type_byte_max_value_writes_and_reads() {
        var field = new DataField<byte>("byte");
        byte actual = await WriteReadSingleAsync(field, byte.MaxValue);
        Assert.Equal(byte.MaxValue, actual);
    }

    [Fact]
    public async Task Type_signed_byte_min_value_writes_and_reads() {
        var field = new DataField<sbyte>("sbyte");
        sbyte actual = await WriteReadSingleAsync(field, sbyte.MinValue);
        Assert.Equal(sbyte.MinValue, actual);
    }

    [Fact]
    public async Task Type_signed_byte_max_value_writes_and_reads() {
        var field = new DataField<sbyte>("sbyte");
        sbyte actual = await WriteReadSingleAsync(field, sbyte.MaxValue);
        Assert.Equal(sbyte.MaxValue, actual);
    }

    [Fact]
    public async Task Type_short_min_value_writes_and_reads() {
        var field = new DataField<short>("short");
        short actual = await WriteReadSingleAsync(field, short.MinValue);
        Assert.Equal(short.MinValue, actual);
    }

    [Fact]
    public async Task Type_short_max_value_writes_and_reads() {
        var field = new DataField<short>("short");
        short actual = await WriteReadSingleAsync(field, short.MaxValue);
        Assert.Equal(short.MaxValue, actual);
    }

    [Fact]
    public async Task Type_unsigned_short_min_value_writes_and_reads() {
        var field = new DataField<ushort>("ushort");
        ushort actual = await WriteReadSingleAsync(field, ushort.MinValue);
        Assert.Equal(ushort.MinValue, actual);
    }

    [Fact]
    public async Task Type_unsigned_short_max_value_writes_and_reads() {
        var field = new DataField<ushort>("ushort");
        ushort actual = await WriteReadSingleAsync(field, ushort.MaxValue);
        Assert.Equal(ushort.MaxValue, actual);
    }

    [Fact]
    public async Task Type_int_min_value_writes_and_reads() {
        var field = new DataField<int>("int");
        int actual = await WriteReadSingleAsync(field, int.MinValue);
        Assert.Equal(int.MinValue, actual);
    }

    [Fact]
    public async Task Type_int_max_value_writes_and_reads() {
        var field = new DataField<int>("int");
        int actual = await WriteReadSingleAsync(field, int.MaxValue);
        Assert.Equal(int.MaxValue, actual);
    }

    [Fact]
    public async Task Type_unsigned_int_min_value_writes_and_reads() {
        var field = new DataField<uint>("uint");
        uint actual = await WriteReadSingleAsync(field, uint.MinValue);
        Assert.Equal(uint.MinValue, actual);
    }

    [Fact]
    public async Task Type_unsigned_int_max_value_writes_and_reads() {
        var field = new DataField<uint>("uint");
        uint actual = await WriteReadSingleAsync(field, uint.MaxValue);
        Assert.Equal(uint.MaxValue, actual);
    }

    [Fact]
    public async Task Type_long_min_value_writes_and_reads() {
        var field = new DataField<long>("long");
        long actual = await WriteReadSingleAsync(field, long.MinValue);
        Assert.Equal(long.MinValue, actual);
    }

    [Fact]
    public async Task Type_long_max_value_writes_and_reads() {
        var field = new DataField<long>("long");
        long actual = await WriteReadSingleAsync(field, long.MaxValue);
        Assert.Equal(long.MaxValue, actual);
    }

    [Fact]
    public async Task Type_unsigned_long_min_value_writes_and_reads() {
        var field = new DataField<ulong>("ulong");
        ulong actual = await WriteReadSingleAsync(field, ulong.MinValue);
        Assert.Equal(ulong.MinValue, actual);
    }

    [Fact]
    public async Task Type_unsigned_long_max_value_writes_and_reads() {
        var field = new DataField<ulong>("ulong");
        ulong actual = await WriteReadSingleAsync(field, ulong.MaxValue);
        Assert.Equal(ulong.MaxValue, actual);
    }

    // --- Nullable tests ---

    [Fact]
    public async Task Type_nullable_decimal_writes_and_reads() {
        var field = new DecimalDataField("decimal?", 4, 1, true, true);
        decimal? actual = await WriteReadSingleNullableAsync<decimal>(field);
        Assert.Null(actual);
    }

    [Fact]
    public async Task Type_nullable_datetime_writes_and_reads() {
        var field = new DateTimeDataField("DateTime?", DateTimeFormat.DateAndTime, isNullable: true);
        DateTime? actual = await WriteReadSingleNullableAsync<DateTime>(field);
        Assert.Null(actual);
    }

    // --- Bool tests ---

    [Fact]
    public async Task Type_bool_writes_and_reads() {
        var field = new DataField<bool>("bool");
        bool actual = await WriteReadSingleAsync(field, true);
        Assert.True(actual);
    }

    [Fact]
    public async Task Type_nullable_bool_writes_and_reads() {
        var field = new DataField<bool?>("bool?");
        bool? actual = await WriteReadSingleNullableAsync<bool>(field, true);
        Assert.Equal(true, actual);
    }

    // --- Guid tests ---

    [Fact]
    public async Task Type_guid_writes_and_reads() {
        var field = new DataField<Guid>("uuid");
        Guid expected = Guid.NewGuid();
        Guid actual = await WriteReadSingleAsync(field, expected);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Type_nullable_guid_not_null_writes_and_reads() {
        var field = new DataField<Guid?>("uuid");
        Guid expected = Guid.NewGuid();
        Guid? actual = await WriteReadSingleNullableAsync<Guid>(field, expected);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Type_nullable_guid_null_writes_and_reads() {
        var field = new DataField<Guid?>("uuid");
        Guid? actual = await WriteReadSingleNullableAsync<Guid>(field);
        Assert.Null(actual);
    }
}