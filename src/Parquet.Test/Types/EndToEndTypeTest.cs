using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using Parquet.File.Values.Primitives;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Schema;

namespace Parquet.Test.Types {
    public static class TypeExtensions {
        public static bool IsArrayOf<T>(this Type type) {
            return type == typeof(T[]);
        }
    }

    public class EndToEndTypeTest : TestBase {
        private static readonly Dictionary<string, (DataField field, object? expectedValue)> _nameToData =
           new Dictionary<string, (DataField field, object? expectedValue)> {
               ["plain string"] = (new DataField<string>("string"), "plain string"),
               ["unicode string"] = (new DataField<string>("unicode string"), "L'Oréal Paris"),
               ["byte array"] = (new DataField<byte[]>("byte array"), Encoding.UTF8.GetBytes("raw byte string")),
               ["float"] = (new DataField<float>("float"), 1.23f),
               ["double"] = (new DataField<double>("double"), 10.44D),
               ["simple DateTime"] = (new DataField<DateTime>("datetime"), DateTime.UtcNow.RoundToSecond()),
               ["long"] = (new DataField<long>("long"), (long)1234),

               //difference cases of decimals
               ["simple decimal"] = (new DataField<decimal>("decDefault"), 123.4m),
               ["huge decimal"] = (new DataField<decimal>("hugeDec"), 83086059037282.54m),
               ["int32 decimal"] = (new DecimalDataField("decInt32", 4, 1), 12.4m),
               ["int64 decimal"] = (new DecimalDataField("decInt64", 17, 12), 1234567.88m),
               ["fixed byte array decimal"] = (new DecimalDataField("decFixedByteArray", 48, 12), 34434.5m),
               ["negative decimal"] = (new DecimalDataField("decMinus", 10, 2, true), -1m),
               ["scale zero"] = (new DecimalDataField("v", 10, 0, true), 10.0m),

               //loses precision slightly, i.e.
               //Expected: 2017-07-13T10:58:44.3767154+00:00
               //Actual:   2017-07-12T10:58:44.3770000+00:00
               ["dateTime"] = (new DataField<DateTime>("dateTime"), DateTime.UtcNow.RoundToSecond()),
               ["impala date"] = (new DateTimeDataField("dateImpala", DateTimeFormat.Impala), DateTime.UtcNow.RoundToSecond()),
               ["dateDateAndTimeMillis"] = (new DateTimeDataField("dateDateAndTime", DateTimeFormat.DateAndTime), DateTime.UtcNow.RoundToMillisecond()),
#if NET7_0_OR_GREATER
               ["dateDateAndTimeMicros"] = (new DateTimeDataField("dateDateAndTime", DateTimeFormat.DateAndTimeMicros), DateTime.UtcNow.RoundToMicrosecond()),
#endif
               ["dateTime unknown kind"] = (new DataField<DateTime>("dateTime unknown kind"), new DateTime(2020, 06, 10, 11, 12, 13)),
               ["impala date unknown kind"] = (new DateTimeDataField("dateImpala unknown kind", DateTimeFormat.Impala), new DateTime(2020, 06, 10, 11, 12, 13)),
               ["dateDateAndTime unknown kind"] = (new DateTimeDataField("dateDateAndTime unknown kind", DateTimeFormat.DateAndTime), new DateTime(2020, 06, 10, 11, 12, 13)),
               ["dateTime local kind"] = (new DataField<DateTime>("dateTime unknown kind"), new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local)),
               ["impala date local kind"] = (new DateTimeDataField("dateImpala unknown kind", DateTimeFormat.Impala), new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local)),
               ["dateDateAndTime local kind"] = (new DateTimeDataField("dateDateAndTime unknown kind", DateTimeFormat.DateAndTime), new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local)),
               ["timestamp utc kind"] = (new DateTimeDataField("timestamp utc kind", DateTimeFormat.Timestamp, true), new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Utc)),
               ["timestamp local kind"] = (new DateTimeDataField("timestamp local kind", DateTimeFormat.Timestamp, false), new DateTime(2020, 06, 10, 11, 12, 13, DateTimeKind.Local)),
               // don't want any excess info in the offset INT32 doesn't contain or care about this data 
               ["dateDate"] = (new DateTimeDataField("dateDate", DateTimeFormat.Date), DateTime.UtcNow.RoundToDay()),
#if !NETCOREAPP3_1
               ["dateOnly"] = (new DataField<DateOnly>("dateOnly"), DateOnly.FromDateTime(DateTime.UtcNow)),
#endif
               ["interval"] = (new DataField<Interval>("interval"), new Interval(3, 2, 1)),
               // time test(loses precision slightly)
               ["time_micros"] = (new TimeSpanDataField("timeMicros", TimeSpanFormat.MicroSeconds), new TimeSpan(DateTime.UtcNow.TimeOfDay.Ticks / 10 * 10)),
               ["time_millis"] = (new TimeSpanDataField("timeMillis", TimeSpanFormat.MilliSeconds), new TimeSpan(DateTime.UtcNow.TimeOfDay.Ticks / 10000 * 10000)),
#if NET6_0_OR_GREATER
               ["timeonly_micros"] = (new TimeOnlyDataField("timeMicros", TimeSpanFormat.MicroSeconds), new TimeOnly(DateTime.UtcNow.TimeOfDay.Ticks / 10 * 10)),
               ["timeonly_millis"] = (new TimeOnlyDataField("timeMillis", TimeSpanFormat.MilliSeconds), new TimeOnly(DateTime.UtcNow.TimeOfDay.Ticks / 10000 * 10000)),
#endif
               
               ["byte min value"] = (new DataField<byte>("byte"), byte.MinValue),
               ["byte max value"] = (new DataField<byte>("byte"), byte.MaxValue),
               ["signed byte min value"] = (new DataField<sbyte>("sbyte"), sbyte.MinValue),
               ["signed byte max value"] = (new DataField<sbyte>("sbyte"), sbyte.MaxValue),

               ["short min value"] = (new DataField<short>("short"), short.MinValue),
               ["short max value"] = (new DataField<short>("short"), short.MaxValue),
               ["unsigned short min value"] = (new DataField<ushort>("ushort"), ushort.MinValue),
               ["unsigned short max value"] = (new DataField<ushort>("ushort"), ushort.MaxValue),

               ["int min value"] = (new DataField<int>("int"), int.MinValue),
               ["int max value"] = (new DataField<int>("int"), int.MaxValue),
               ["unsigned int min value"] = (new DataField<uint>("uint"), uint.MinValue),
               ["unsigned int max value"] = (new DataField<uint>("uint"), uint.MaxValue),

               ["long min value"] = (new DataField<long>("long"), long.MinValue),
               ["long max value"] = (new DataField<long>("long"), long.MaxValue),
               ["unsigned long min value"] = (new DataField<ulong>("ulong"), ulong.MinValue),
               ["unsigned long max value"] = (new DataField<ulong>("ulong"), ulong.MaxValue),

               ["nullable decimal"] = (new DecimalDataField("decimal?", 4, 1, true, true), null),
               ["nullable DateTime"] = (new DateTimeDataField("DateTime?", DateTimeFormat.DateAndTime, isNullable: true), null),

               ["bool"] = (new DataField<bool>("bool"), true),
               ["nullable bool"] = (new DataField<bool?>("bool?"), new bool?(true)),

               ["guid"] = (new DataField<Guid>("uuid"), Guid.NewGuid()),
               ["nullable guid (not null)"] = (new DataField<Guid?>("uuid"), Guid.NewGuid()),
               ["nullable guid (null)"] = (new DataField<Guid?>("uuid"), null)

           };

        [Theory]

        [InlineData("plain string")]
        [InlineData("unicode string")]
        [InlineData("byte array")]
        [InlineData("float")]
        [InlineData("double")]
        [InlineData("simple DateTime")]
        [InlineData("long")]

        [InlineData("simple decimal")]
        [InlineData("huge decimal")]
        [InlineData("int32 decimal")]
        [InlineData("int64 decimal")]
        [InlineData("fixed byte array decimal")]
        [InlineData("negative decimal")]

        [InlineData("dateTime")]
        [InlineData("impala date")]
        [InlineData("dateDateAndTimeMillis")]
#if NET7_0_OR_GREATER
        [InlineData("dateDateAndTimeMicros")]
#endif
        [InlineData("dateTime unknown kind")]
        [InlineData("impala date unknown kind")]
        [InlineData("dateDateAndTime unknown kind")]
        [InlineData("dateTime local kind")]
        [InlineData("impala date local kind")]
        [InlineData("dateDateAndTime local kind")]
        [InlineData("timestamp utc kind")]
        [InlineData("timestamp local kind")]
        [InlineData("dateDate")]
#if !NETCOREAPP3_1
        [InlineData("dateOnly")]
#endif
        [InlineData("interval")]
        [InlineData("time_micros")]
        [InlineData("time_millis")]
#if NET6_0_OR_GREATER
        [InlineData("timeonly_micros")]
        [InlineData("timeonly_millis")]
#endif

        [InlineData("byte min value")]
        [InlineData("byte max value")]
        [InlineData("signed byte min value")]
        [InlineData("signed byte max value")]
        [InlineData("short min value")]
        [InlineData("short max value")]
        [InlineData("unsigned short min value")]
        [InlineData("unsigned short max value")]

        [InlineData("int min value")]
        [InlineData("int max value")]
        [InlineData("unsigned int min value")]
        [InlineData("unsigned int max value")]

        [InlineData("long min value")]
        [InlineData("long max value")]
        [InlineData("unsigned long min value")]
        [InlineData("unsigned long max value")]

        [InlineData("nullable decimal")]
        [InlineData("nullable DateTime")]

        [InlineData("bool")]
        [InlineData("nullable bool")]
        [InlineData("guid")]
        [InlineData("nullable guid (null)")]
        [InlineData("nullable guid (not null)")]

        public async Task Type_writes_and_reads_end_to_end(string name) {
            (DataField field, object? expectedValue) input = _nameToData[name];

            object actual = await WriteReadSingle(input.field, input.expectedValue);

            bool equal;
            if(input.expectedValue == null && actual == null)
                equal = true;
            else if(actual.GetType().IsArrayOf<byte>() && input.expectedValue != null) {
                equal = ((byte[])actual).SequenceEqual((byte[])input.expectedValue);
            } else if(input.field is DateTimeDataField { DateTimeFormat: DateTimeFormat.Timestamp }) {
                var dtActual = (DateTime)actual;
                var dtExpected = (DateTime)input.expectedValue!;
                Assert.Equal(dtExpected.Kind, dtActual.Kind);
                equal = dtActual.Equals(dtExpected);
            } else if(actual.GetType() == typeof(DateTime)) {
                var dtActual = (DateTime)actual;
                Assert.Equal(DateTimeKind.Utc, dtActual.Kind);
                var dtExpected = (DateTime)input.expectedValue!;
                dtExpected = dtExpected.Kind == DateTimeKind.Unspecified
                    ? DateTime.SpecifyKind(dtExpected, DateTimeKind.Utc) // assumes value is UTC
                    : dtExpected.ToUniversalTime();
                equal = dtActual.Equals(dtExpected);
            } else {
                equal = actual.Equals(input.expectedValue);
            }

            Assert.True(equal, $"{name}| expected: [{input.expectedValue}], actual: [{actual}], schema element: {input.field}");
        }

    }
}