using System;
using System.IO;
using Parquet.Data;
using System.Linq;
using F = System.IO.File;
using System.Collections.Generic;
using System.Threading.Tasks;
using Parquet.Extensions;
using Parquet.Schema;
using System.Text;
using Parquet.File.Values.Primitives;
using System.Reflection;
using Xunit.Sdk;

namespace Parquet.Test {
    public class TestBase {

        public sealed record TypeTestInstance(string Name, DataField Field, object? ExpectedValue, bool DuckDbSupported = true);

        // xUnit data attribute that yields each NameFieldExpected as a single theory parameter
        public sealed class TypeTestDataAttribute : DataAttribute {

            public bool DuckDb { get; set;  } = false;

            public override IEnumerable<object?[]> GetData(MethodInfo testMethod) {

                IEnumerable<TypeTestInstance> source = new List<TypeTestInstance>(TypeTests);

                if(DuckDb)
                    source = source.Where(e => e.DuckDbSupported);

                foreach(TypeTestInstance nfe in source) {
                    yield return new object?[] { nfe };
                }
            }
        }

        public static readonly List<TypeTestInstance> TypeTests =
           new List<TypeTestInstance> {
               new TypeTestInstance("plain string", new DataField<string>("string"), "plain string"),
               new TypeTestInstance("unicode string", new DataField<string>("unicode string"), "L'Oréal Paris"),
               new TypeTestInstance("byte array", new DataField<byte[]>("byte array"), Encoding.UTF8.GetBytes("raw byte string")),
               new TypeTestInstance("float", new DataField<float>("float"), 1.23f),
               new TypeTestInstance("double", new DataField<double>("double"), 10.44D),
               new TypeTestInstance("simple DateTime", new DataField<DateTime>("datetime"), DateTime.UtcNow.RoundToSecond()),
               new TypeTestInstance("long", new DataField<long>("long"), (long)1234),

               //difference cases of decimals
               new TypeTestInstance("simple decimal", new DataField<decimal>("decDefault"), 123.4m),
               new TypeTestInstance("huge decimal", new DataField<decimal>("hugeDec"), 83086059037282.54m),
               new TypeTestInstance("int32 decimal", new DecimalDataField("decInt32", 4, 1), 12.4m),
               new TypeTestInstance("int64 decimal", new DecimalDataField("decInt64", 17, 12), 1234567.88m, DuckDbSupported: false),
               new TypeTestInstance("fixed byte array decimal", new DecimalDataField("decFixedByteArray", 48, 12), 34434.5m, DuckDbSupported: false),
               new TypeTestInstance("negative decimal", new DecimalDataField("decMinus", 10, 2, true), -1m),
               new TypeTestInstance("scale zero", new DecimalDataField("v", 10, 0, true), 10.0m),

               //loses precision slightly, i.e.
               //Expected: 2017-07-13T10:58:44.3767154+00:00
               //Actual:   2017-07-12T10:58:44.3770000+00:00
               new TypeTestInstance("dateTime", new DataField<DateTime>("dateTime"), DateTime.UtcNow.RoundToSecond()),
               new TypeTestInstance("impala date", new DateTimeDataField("dateImpala", DateTimeFormat.Impala), DateTime.UtcNow.RoundToSecond()),
               new TypeTestInstance("dateDateAndTimeMillis", new DateTimeDataField("dateDateAndTime", DateTimeFormat.DateAndTime), DateTime.UtcNow.RoundToMillisecond()),
#if NET7_0_OR_GREATER
               new TypeTestInstance("dateDateAndTimeMicros", new DateTimeDataField("dateDateAndTime", DateTimeFormat.DateAndTimeMicros), DateTime.UtcNow.RoundToMicrosecond()),
#endif
               new TypeTestInstance("dateTime unknown kind", new DataField<DateTime>("dateTime unknown kind"), new DateTime(2020, 06, 10, 11, 12, 13)),
               new TypeTestInstance("impala date unknown kind", new DateTimeDataField("dateImpala unknown kind", DateTimeFormat.Impala), new DateTime(2020, 06, 10, 11, 12, 13)),
               new TypeTestInstance("dateDateAndTime unknown kind", new DateTimeDataField("dateDateAndTime unknown kind", DateTimeFormat.DateAndTime), new DateTime(2020, 06, 10, 11, 12, 13)),
               new TypeTestInstance("dateTime local kind", new DataField<DateTime>("dateTime unknown kind"), new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local)),
               new TypeTestInstance("impala date local kind", new DateTimeDataField("dateImpala unknown kind", DateTimeFormat.Impala), new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local)),
               new TypeTestInstance("dateDateAndTime local kind", new DateTimeDataField("dateDateAndTime unknown kind", DateTimeFormat.DateAndTime), new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local)),
               new TypeTestInstance("timestamp utc kind", new DateTimeDataField("timestamp utc kind", DateTimeFormat.Timestamp, true), new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Utc)),
               new TypeTestInstance("timestamp local kind", new DateTimeDataField("timestamp local kind", DateTimeFormat.Timestamp, false), new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local)),
               // don't want any excess info in the offset INT32 doesn't contain or care about this data 
               new TypeTestInstance("dateDate", new DateTimeDataField("dateDate", DateTimeFormat.Date), DateTime.UtcNow.RoundToDay()),
#if !NETCOREAPP3_1
               new TypeTestInstance("dateOnly", new DataField<DateOnly>("dateOnly"), DateOnly.FromDateTime(DateTime.UtcNow)),
#endif
               new TypeTestInstance("interval", new DataField<Interval>("interval"), new Interval(3, 2, 1)),
               // time test(loses precision slightly)
               new TypeTestInstance("time_micros", new TimeSpanDataField("timeMicros", TimeSpanFormat.MicroSeconds), new TimeSpan(DateTime.UtcNow.TimeOfDay.Ticks / 10 * 10)),
               new TypeTestInstance("time_millis", new TimeSpanDataField("timeMillis", TimeSpanFormat.MilliSeconds), new TimeSpan(DateTime.UtcNow.TimeOfDay.Ticks / 10000 * 10000)),
#if NET6_0_OR_GREATER
               new TypeTestInstance("timeonly_micros", new TimeOnlyDataField("timeMicros", TimeSpanFormat.MicroSeconds), new TimeOnly(DateTime.UtcNow.TimeOfDay.Ticks / 10 * 10)),
               new TypeTestInstance("timeonly_millis", new TimeOnlyDataField("timeMillis", TimeSpanFormat.MilliSeconds), new TimeOnly(DateTime.UtcNow.TimeOfDay.Ticks / 10000 * 10000)),
#endif

               new TypeTestInstance("byte min value", new DataField<byte>("byte"), byte.MinValue),
               new TypeTestInstance("byte max value", new DataField<byte>("byte"), byte.MaxValue),
               new TypeTestInstance("signed byte min value", new DataField<sbyte>("sbyte"), sbyte.MinValue),
               new TypeTestInstance("signed byte max value", new DataField<sbyte>("sbyte"), sbyte.MaxValue),

               new TypeTestInstance("short min value", new DataField<short>("short"), short.MinValue),
               new TypeTestInstance("short max value", new DataField<short>("short"), short.MaxValue),
               new TypeTestInstance("unsigned short min value", new DataField<ushort>("ushort"), ushort.MinValue),
               new TypeTestInstance("unsigned short max value", new DataField<ushort>("ushort"), ushort.MaxValue),

               new TypeTestInstance("int min value", new DataField<int>("int"), int.MinValue),
               new TypeTestInstance("int max value", new DataField<int>("int"), int.MaxValue),
               new TypeTestInstance("unsigned int min value", new DataField<uint>("uint"), uint.MinValue),
               new TypeTestInstance("unsigned int max value", new DataField<uint>("uint"), uint.MaxValue),

               new TypeTestInstance("long min value", new DataField<long>("long"), long.MinValue),
               new TypeTestInstance("long max value", new DataField<long>("long"), long.MaxValue),
               new TypeTestInstance("unsigned long min value", new DataField<ulong>("ulong"), ulong.MinValue),
               new TypeTestInstance("unsigned long max value", new DataField<ulong>("ulong"), ulong.MaxValue),

               new TypeTestInstance("nullable decimal", new DecimalDataField("decimal?", 4, 1, true, true), null),
               new TypeTestInstance("nullable DateTime", new DateTimeDataField("DateTime?", DateTimeFormat.DateAndTime, isNullable: true), null),

               new TypeTestInstance("bool", new DataField<bool>("bool"), true),
               new TypeTestInstance("nullable bool", new DataField<bool?>("bool?"), new bool?(true)),

               new TypeTestInstance("guid", new DataField<Guid>("uuid"), Guid.NewGuid()),
               new TypeTestInstance("nullable guid (not null)", new DataField<Guid?>("uuid"), Guid.NewGuid()),
               new TypeTestInstance("nullable guid (null)", new DataField<Guid?>("uuid"), null)

           };


        protected Stream OpenTestFile(string name) {
            return F.OpenRead("./data/" + name);
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
}