using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;
using Parquet.File.Values.Primitives;
using NetBox.Extensions;
using System.Linq;

namespace Parquet.Test
{
   public static class TypeExtensions
   {
      public static bool IsArrayOf<T>(this Type type)
      {
         return type == typeof(T[]);
      }
   }

   public class EndToEndTypeTest : TestBase
   {
      private static Dictionary<string, (DataField field, object expectedValue)> _nameToData =
         new Dictionary<string, (DataField field, object expectedValue)>
         {
            ["plain string"] = (new DataField<string>("string"), "plain string"),
            ["unicode string"] = (new DataField<string>("unicode string"), "L'Oréal Paris"),
            ["byte array"] = (new DataField<byte[]>("byte array"), Encoding.UTF8.GetBytes("raw byte string")),
            ["float"] = (new DataField<float>("float"), 1.23f),
            ["double"] = (new DataField<double>("double"), 10.44D),
            ["simple DateTime"] = (new DataField<DateTime>("datetime"), new DateTimeOffset(DateTime.UtcNow.RoundToSecond())),
            ["long"] = (new DataField<long>("long"), (long)1234),

            //difference cases of decimals
            ["simple decimal"] = (new DataField<decimal>("decDefault"), 123.4m),
            ["huge decimal"] = (new DataField<decimal>("hugeDec"), 83086059037282.54m),
            ["int32 decimal"] = (new DecimalDataField("decInt32", 4, 1), 12.4m),
            ["int64 decimal"] = (new DecimalDataField("decInt64", 17, 12), 1234567.88m),
            ["fixed byte array decimal"] = (new DecimalDataField("decFixedByteArray", 48, 12), 34434.5m),
            ["negative decimal"] = (new DecimalDataField("decMinus", 10, 2, true), -1m),

            //loses precision slightly, i.e.
            //Expected: 2017-07-13T10:58:44.3767154+00:00
            //Actual:   2017-07-12T10:58:44.3770000+00:00
            ["dateTimeOffset"] = (new DataField<DateTimeOffset>("dateTimeOffset"), new DateTimeOffset(DateTime.UtcNow.RoundToSecond())),
            ["impala date"] = (new DateTimeDataField("dateImpala", DateTimeFormat.Impala), new DateTimeOffset(DateTime.UtcNow.RoundToSecond())),
            ["dateDateAndTime"] = (new DateTimeDataField("dateDateAndTime", DateTimeFormat.DateAndTime), new DateTimeOffset(DateTime.UtcNow.RoundToSecond())),
            // don't want any excess info in the offset INT32 doesn't contain or care about this data 
            ["dateDate"] = (new DateTimeDataField("dateDate", DateTimeFormat.Date), new DateTimeOffset(DateTime.UtcNow.RoundToDay(), TimeSpan.Zero)),
            ["interval"] = (new DataField<Interval>("interval"), new Interval(3, 2, 1)),


            ["byte min value"] = (new DataField<byte>("byte"), byte.MinValue),
            ["byte max value"] = (new DataField<byte>("byte"), byte.MaxValue),
            ["signed byte min value"] = (new DataField<sbyte>("sbyte"), sbyte.MinValue),
            ["signed byte max value"] = (new DataField<sbyte>("sbyte"), sbyte.MaxValue),

            ["short min value"] = (new DataField<short>("short"), short.MinValue),
            ["short max value"] = (new DataField<short>("short"), short.MaxValue),
            ["unsigned short min value"] = (new DataField<ushort>("ushort"), ushort.MinValue),
            ["unsigned short max value"] = (new DataField<ushort>("ushort"), ushort.MaxValue),

            ["nullable decimal"] = (new DecimalDataField("decimal?", 4, 1, true, true), null),
            ["nullable DateTime"] = (new DateTimeDataField("DateTime?", DateTimeFormat.DateAndTime, true), null),

            ["bool"] = (new DataField<bool>("bool"), true),
            ["nullable bool"] = (new DataField<bool?>("bool?"), new bool?(true))

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

      [InlineData("dateTimeOffset")]
      [InlineData("impala date")]
      [InlineData("dateDateAndTime")]
      [InlineData("dateDate")]
      [InlineData("interval")]

      [InlineData("byte min value")]
      [InlineData("byte max value")]
      [InlineData("signed byte min value")]
      [InlineData("signed byte max value")]
      [InlineData("short min value")]
      [InlineData("short max value")]
      [InlineData("unsigned short min value")]
      [InlineData("unsigned short max value")]

      [InlineData("nullable decimal")]
      [InlineData("nullable DateTime")]

      [InlineData("bool")]
      [InlineData("nullable bool")]

      public void Type_writes_and_reads_end_to_end(string name)
      {
         (DataField field, object expectedValue) input = _nameToData[name];

         object actual = WriteReadSingle(input.field, input.expectedValue);

         bool equal;
         if (input.expectedValue == null && actual == null)
         {
            equal = true;
         }
         else if (actual.GetType().IsArrayOf<byte>() && input.expectedValue != null)
         {
            equal = ((byte[]) actual).SequenceEqual((byte[]) input.expectedValue);
         }
         else
         {
            equal = actual.Equals(input.expectedValue);
         }

         Assert.True(equal, $"{name}| expected: [{input.expectedValue}], actual: [{actual}], schema element: {input.field}"); 
      }

   }
}
