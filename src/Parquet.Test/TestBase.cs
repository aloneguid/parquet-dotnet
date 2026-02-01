using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Extensions;
using Parquet.File.Values.Primitives;
using Parquet.Schema;
using Parquet.Test.Util;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;
using F = System.IO.File;

namespace Parquet.Test;

public class TestBase {

    public sealed record TTI(string Name, DataField Field, object? ExpectedValue, bool DuckDbSupported = true);

    /// <summary>
    /// xUnit data attribute that yields each NameFieldExpected as a single theory parameter
    /// </summary>
    public sealed class TypeTestDataAttribute : DataAttribute {

        public bool DuckDb { get; set; } = false;

        public override ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker) {
            IEnumerable<TTI> source = TypeTests;

            if(DuckDb)
                source = source.Where(e => e.DuckDbSupported);

            IReadOnlyCollection<ITheoryDataRow> data =
                source.Select(tti => new TheoryDataRow(tti)).ToList();
            
            return ValueTask.FromResult(data);
        }

        public override bool SupportsDiscoveryEnumeration() => true;
    }

    public static readonly TTI[] TypeTests = [
        new TTI("plain string", new DataField<string>("string"), "plain string"),
        new TTI("unicode string", new DataField<string>("unicode string"), "L'Oréal Paris"),
        new TTI("byte array", new DataField<byte[]>("byte array"), Encoding.UTF8.GetBytes("raw byte string")),
        new TTI("float", new DataField<float>("float"), 1.23f),
        new TTI("double", new DataField<double>("double"), 10.44D),
        new TTI("simple DateTime", new DataField<DateTime>("datetime"), DateTime.UtcNow.RoundToSecond()),
        new TTI("long", new DataField<long>("long"), (long)1234),

        //difference cases of decimals
        new TTI("simple decimal", new DataField<decimal>("decDefault"), 123.4m),
        new TTI("huge decimal", new DataField<decimal>("hugeDec"), 83086059037282.54m),
        new TTI("int32 decimal", new DecimalDataField("decInt32", 4, 1), 12.4m),
        new TTI("int64 decimal", new DecimalDataField("decInt64", 17, 12), 1234567.88m, DuckDbSupported: false),
        new TTI("fixed byte array decimal", new DecimalDataField("decFixedByteArray", 48, 12), 34434.5m,
            DuckDbSupported: false),
        new TTI("negative decimal", new DecimalDataField("decMinus", 10, 2, true), -1m),
        new TTI("scale zero", new DecimalDataField("v", 10, 0, true), 10.0m),
        new TTI("really big decimal", new DecimalDataField("decReallyBig", 38, 2, clrType: typeof(BigDecimal)),
            new BigDecimal(BigInteger.Parse("12345678901234567890123456789012345678"), 38, 2), DuckDbSupported: false),
        new TTI("really big bigdecimal", new BigDecimalDataField("decReallyBig", 38, 2),
            new BigDecimal(BigInteger.Parse("12345678901234567890123456789012345678"), 38, 2), DuckDbSupported: false),


        //loses precision slightly, i.e.
        //Expected: 2017-07-13T10:58:44.3767154+00:00
        //Actual:   2017-07-12T10:58:44.3770000+00:00
        new TTI("dateTime", new DataField<DateTime>("dateTime"), DateTime.UtcNow.RoundToSecond()),
        new TTI("impala date", new DateTimeDataField("dateImpala", DateTimeFormat.Impala),
            DateTime.UtcNow.RoundToSecond()),
        new TTI("dateDateAndTimeMillis", new DateTimeDataField("dateDateAndTime", DateTimeFormat.DateAndTime),
            DateTime.UtcNow.RoundToMillisecond()),
#if NET7_0_OR_GREATER
        new TTI("dateDateAndTimeMicros", new DateTimeDataField("dateDateAndTime", DateTimeFormat.DateAndTimeMicros),
            DateTime.UtcNow.RoundToMicrosecond()),
#endif
        new TTI("dateTime unknown kind", new DataField<DateTime>("dateTime unknown kind"),
            new DateTime(2020, 06, 10, 11, 12, 13)),
        new TTI("impala date unknown kind", new DateTimeDataField("dateImpala unknown kind", DateTimeFormat.Impala),
            new DateTime(2020, 06, 10, 11, 12, 13)),
        new TTI("dateDateAndTime unknown kind",
            new DateTimeDataField("dateDateAndTime unknown kind", DateTimeFormat.DateAndTime),
            new DateTime(2020, 06, 10, 11, 12, 13)),
        new TTI("dateTime local kind", new DataField<DateTime>("dateTime unknown kind"),
            new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local)),
        new TTI("impala date local kind", new DateTimeDataField("dateImpala unknown kind", DateTimeFormat.Impala),
            new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local)),
        new TTI("dateDateAndTime local kind",
            new DateTimeDataField("dateDateAndTime unknown kind", DateTimeFormat.DateAndTime),
            new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local)),
        new TTI("timestamp utc kind", new DateTimeDataField("timestamp utc kind", DateTimeFormat.Timestamp, true),
            new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Utc)),
        new TTI("timestamp local kind", new DateTimeDataField("timestamp local kind", DateTimeFormat.Timestamp, false),
            new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local)),
        // don't want any excess info in the offset INT32 doesn't contain or care about this data 
        new TTI("dateDate", new DateTimeDataField("dateDate", DateTimeFormat.Date), DateTime.UtcNow.RoundToDay()),
#if !NETCOREAPP3_1
        new TTI("dateOnly", new DataField<DateOnly>("dateOnly"), DateOnly.FromDateTime(DateTime.UtcNow)),
#endif
        new TTI("interval", new DataField<Interval>("interval"), new Interval(3, 2, 1)),
        // time test(loses precision slightly)
        new TTI("time_micros", new TimeSpanDataField("timeMicros", TimeSpanFormat.MicroSeconds),
            new TimeSpan(DateTime.UtcNow.TimeOfDay.Ticks / 10 * 10)),
        new TTI("time_millis", new TimeSpanDataField("timeMillis", TimeSpanFormat.MilliSeconds),
            new TimeSpan(DateTime.UtcNow.TimeOfDay.Ticks / 10000 * 10000)),
#if NET6_0_OR_GREATER
        new TTI("timeonly_micros", new TimeOnlyDataField("timeMicros", TimeSpanFormat.MicroSeconds),
            new TimeOnly(DateTime.UtcNow.TimeOfDay.Ticks / 10 * 10)),
        new TTI("timeonly_millis", new TimeOnlyDataField("timeMillis", TimeSpanFormat.MilliSeconds),
            new TimeOnly(DateTime.UtcNow.TimeOfDay.Ticks / 10000 * 10000)),
#endif

        new TTI("byte min value", new DataField<byte>("byte"), byte.MinValue),
        new TTI("byte max value", new DataField<byte>("byte"), byte.MaxValue),
        new TTI("signed byte min value", new DataField<sbyte>("sbyte"), sbyte.MinValue),
        new TTI("signed byte max value", new DataField<sbyte>("sbyte"), sbyte.MaxValue),

        new TTI("short min value", new DataField<short>("short"), short.MinValue),
        new TTI("short max value", new DataField<short>("short"), short.MaxValue),
        new TTI("unsigned short min value", new DataField<ushort>("ushort"), ushort.MinValue),
        new TTI("unsigned short max value", new DataField<ushort>("ushort"), ushort.MaxValue),

        new TTI("int min value", new DataField<int>("int"), int.MinValue),
        new TTI("int max value", new DataField<int>("int"), int.MaxValue),
        new TTI("unsigned int min value", new DataField<uint>("uint"), uint.MinValue),
        new TTI("unsigned int max value", new DataField<uint>("uint"), uint.MaxValue),

        new TTI("long min value", new DataField<long>("long"), long.MinValue),
        new TTI("long max value", new DataField<long>("long"), long.MaxValue),
        new TTI("unsigned long min value", new DataField<ulong>("ulong"), ulong.MinValue),
        new TTI("unsigned long max value", new DataField<ulong>("ulong"), ulong.MaxValue),

        new TTI("nullable decimal", new DecimalDataField("decimal?", 4, 1, true, true), null),
        new TTI("nullable DateTime", new DateTimeDataField("DateTime?", DateTimeFormat.DateAndTime, isNullable: true),
            null),

        new TTI("bool", new DataField<bool>("bool"), true),
        new TTI("nullable bool", new DataField<bool?>("bool?"), new bool?(true)),

        new TTI("guid", new DataField<Guid>("uuid"), Guid.NewGuid()),
        new TTI("nullable guid (not null)", new DataField<Guid?>("uuid"), Guid.NewGuid()),
        new TTI("nullable guid (null)", new DataField<Guid?>("uuid"), null)
    ];

    protected Stream OpenTestFile(string name) {
        return F.OpenRead("./data/" + name);
    }

    protected StreamReader OpenTestFileReader(string name) {
        return new StreamReader("./data/" + name);
    }

    protected async Task<DataColumn?> WriteReadSingleColumn(DataColumn dataColumn) {
        using var ms = new MemoryStream();
        // write with built-in extension method
        await ms.WriteSingleRowGroupParquetFileAsync(new ParquetSchema(dataColumn.Field), dataColumn);
        ms.Position = 0;

        //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

        // read first gow group and first column
        using ParquetReader reader = await ParquetReader.CreateAsync(ms);
        if(reader.RowGroupCount == 0)
            return null;
        ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0);

        return await rgReader.ReadColumnAsync(dataColumn.Field);
    }

    protected async Task<Tuple<DataColumn[], ParquetSchema>> WriteReadSingleRowGroup(
        ParquetSchema schema, DataColumn[] columns) {
        ParquetSchema readSchema;
        using var ms = new MemoryStream();
        await ms.WriteSingleRowGroupParquetFileAsync(schema, columns);
        ms.Position = 0;

        //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

        using ParquetReader reader = await ParquetReader.CreateAsync(ms);
        readSchema = reader.Schema;

        using ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0);
        return Tuple.Create(await columns.Select(c =>
           rgReader.ReadColumnAsync(c.Field))
           .SequentialWhenAll(), readSchema);
    }

    protected async Task<object> WriteReadSingle(DataField field, object? value, CompressionMethod compressionMethod = CompressionMethod.None) {
        //for sanity, use disconnected streams
        byte[] data;

        var options = new ParquetOptions();
#if !NETCOREAPP3_1
        if(value is DateOnly)
            options.UseDateOnlyTypeForDates = true;
#endif

        using(var ms = new MemoryStream()) {
            // write single value

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), ms, options)) {
                writer.CompressionMethod = compressionMethod;

                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                Array dataArray = Array.CreateInstance(field.ClrNullableIfHasNullsType, 1);
                dataArray.SetValue(value, 0);
                var column = new DataColumn(field, dataArray);

                await rg.WriteColumnAsync(column);
            }

            data = ms.ToArray();
        }

        using(var ms = new MemoryStream(data)) {
            // read back single value

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
            DataColumn column = await rowGroupReader.ReadColumnAsync(field);

            return column.Data.GetValue(0)!;
        }
    }

    protected async Task<List<DataColumn>> ReadColumns(Stream s) {
        using ParquetReader reader = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
        var r = new List<DataColumn>();
        foreach(DataField df in reader.Schema.DataFields) {
            DataColumn dc = await rgr.ReadColumnAsync(df);
            r.Add(dc);
        }
        return r;
    }
}