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
         new object[] {  new SchemaElement<string>("s"), "plain string" },
         new object[] {  new SchemaElement<string>("s"), "L'Oréal Paris" },
         new object[] {  new SchemaElement<float>("f"), 1.23f },
         new object[] {  new SchemaElement<double>("d"), 10.44D },
         new object[] { new SchemaElement<DateTime>("datetime"), DateTime.UtcNow.RoundToSecond()},
         new object[] { new SchemaElement<decimal>("dec"), (decimal)123.4 },
         new object[] { new SchemaElement<long>("long"), (long)1234 },

         //loses precision slightly, i.e.
         //Expected: 2017-07-13T10:58:44.3767154+00:00
         //Actual:   2017-07-12T10:58:44.3770000+00:00
         new object[] {  new SchemaElement<DateTimeOffset>("d"), new DateTimeOffset(DateTime.UtcNow.RoundToSecond()), "default" },
         new object[] {  new DateTimeSchemaElement("d", DateTimeFormat.Impala), new DateTimeOffset(DateTime.UtcNow.RoundToSecond()), "impala" },

         new object[] {  new DateTimeSchemaElement("d", DateTimeFormat.DateAndTime), new DateTimeOffset(DateTime.UtcNow.RoundToSecond()), "dateandtime" },
         // don't want any excess info in the offset INT32 doesn't contain or care about this data 
         new object[] {  new DateTimeSchemaElement("d", DateTimeFormat.Date), new DateTimeOffset(DateTime.UtcNow.RoundToDay(), TimeSpan.Zero), "date" },
         new object[] {  new IntervalSchemaElement("interval"), new Interval(3, 2, 1) }
      };

      [Theory]
      [MemberData(nameof(TypeData))]
      public void Type_writes_and_reads_end_to_end(SchemaElement schema, object value, string name = null)
      {
         var ds = new DataSet(schema) { new Row(value) };
         var ms = new MemoryStream();
         ParquetWriter.Write(ds, ms);

         ms.Position = 0;
         DataSet ds1 = ParquetReader.Read(ms);

         object expectedValue = ds[0][0];
         object actualValue = ds1[0][0];

         if(schema.ElementType == typeof(DateTime))
            actualValue = ((DateTimeOffset) actualValue).DateTime;

         Assert.True(expectedValue.Equals(actualValue),
            $"{name}| expected: {expectedValue}, actual: {actualValue}, schema element: {schema}");

         //if (schema.ElementType == typeof(decimal)) ParquetWriter.WriteFile(ds1, "c:\\tmp\\decimals.parquet");
      }
   }
}
