using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.File.Values;
using Xunit;
using Xunit.Extensions;
using Parquet.File.Values.Primitives;

namespace Parquet.Test
{
   public class EndToEndTypeTest
   {
      public static IEnumerable<object[]> TypeData => new[]
      {
         new object[] {  new Field<string>("string"), "plain string" },
         new object[] {  new Field<string>("unicode string"), "L'Oréal Paris" },
         new object[] {  new Field<float>("float"), 1.23f },
         new object[] {  new Field<double>("double"), 10.44D },
         new object[] { new Field<DateTime>("datetime"), DateTime.UtcNow.RoundToSecond()},
         new object[] { new Field<long>("long"), (long)1234 },

         //difference cases of decimals
         new object[] { new Field<decimal>("decDefault"), 123.4m },
         new object[] { new Field<decimal>("hugeDec"), 83086059037282.54m },
         new object[] { new DecimalDataField("decInt32", 4, 1), 12.4m},
         new object[] { new DecimalDataField("decInt64", 17, 12), 1234567.88m},
         new object[] { new DecimalDataField("decFixedByteArray", 48, 12), 34434.5m},

         //loses precision slightly, i.e.
         //Expected: 2017-07-13T10:58:44.3767154+00:00
         //Actual:   2017-07-12T10:58:44.3770000+00:00
         new object[] {  new Field<DateTimeOffset>("dateTimeOffset"), new DateTimeOffset(DateTime.UtcNow.RoundToSecond()), "default" },
         new object[] {  new DateTimeDataField("dateImpala", DateTimeFormat.Impala), new DateTimeOffset(DateTime.UtcNow.RoundToSecond()), "impala" },

         new object[] {  new DateTimeDataField("dateDateAndTime", DateTimeFormat.DateAndTime), new DateTimeOffset(DateTime.UtcNow.RoundToSecond()), "dateandtime" },
         // don't want any excess info in the offset INT32 doesn't contain or care about this data 
         new object[] {  new DateTimeDataField("dateDate", DateTimeFormat.Date), new DateTimeOffset(DateTime.UtcNow.RoundToDay(), TimeSpan.Zero), "date" },
         new object[] {  new Field<Interval>("interval"), new Interval(3, 2, 1) },

         new object[] {  new Field<byte>("byte"), byte.MinValue },
         new object[] {  new Field<byte>("byte"), byte.MaxValue },
         new object[] {  new Field<sbyte>("sbyte"), sbyte.MinValue },
         new object[] {  new Field<sbyte>("sbyte"), sbyte.MaxValue },

         new object[] {  new Field<short>("short"), short.MinValue },
         new object[] {  new Field<short>("short"), short.MaxValue },
         new object[] {  new Field<ushort>("ushort"), ushort.MinValue },
         new object[] {  new Field<ushort>("ushort"), ushort.MaxValue },

         new object[] {  new DecimalDataField("decimal?", 4, 1, true, true), null},
         new object[] {  new DateTimeDataField("DateTime?", DateTimeFormat.DateAndTime, true), null },
      };

      [Theory]
      [MemberData(nameof(TypeData))]
      public void Type_writes_and_reads_end_to_end(Field schema, object value, string name = null)
      {
         var ds = new DataSet(schema) { new Row(value) };
         var ms = new MemoryStream();
         ParquetWriter.Write(ds, ms);

         ms.Position = 0;
         DataSet ds1 = ParquetReader.Read(ms);

         object expectedValue = ds[0][0];
         object actualValue = ds1[0][0];

         //if(schema.DataType == DataType.DateTimeOffset)
         //   actualValue = ((DateTimeOffset) actualValue).DateTime;

         Assert.True(expectedValue == null && actualValue == null || expectedValue.Equals(actualValue),
            $"{name}| expected: {expectedValue}, actual: {actualValue}, schema element: {schema}");
      }
   }
}
