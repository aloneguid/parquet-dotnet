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
         new object[] { new SchemaElement<long>("long"), (long)1234 },

         //difference cases of decimals
         new object[] { new SchemaElement<decimal>("decDefault"), 123.4m },
         new object[] { new DecimalSchemaElement("decInt32", 4, 1), 12.4m},
         new object[] { new DecimalSchemaElement("decInt64", 17, 12), 1234567.88m},
         new object[] { new DecimalSchemaElement("decFixedByteArray", 48, 12), 34434.5m},

         //loses precision slightly, i.e.
         //Expected: 2017-07-13T10:58:44.3767154+00:00
         //Actual:   2017-07-12T10:58:44.3770000+00:00
         new object[] {  new SchemaElement<DateTimeOffset>("d"), new DateTimeOffset(DateTime.UtcNow.RoundToSecond()), "default" },
         new object[] {  new DateTimeSchemaElement("d", DateTimeFormat.Impala), new DateTimeOffset(DateTime.UtcNow.RoundToSecond()), "impala" },

         new object[] {  new DateTimeSchemaElement("d", DateTimeFormat.DateAndTime), new DateTimeOffset(DateTime.UtcNow.RoundToSecond()), "dateandtime" },
         // don't want any excess info in the offset INT32 doesn't contain or care about this data 
         new object[] {  new DateTimeSchemaElement("d", DateTimeFormat.Date), new DateTimeOffset(DateTime.UtcNow.RoundToDay(), TimeSpan.Zero), "date" },
         new object[] {  new IntervalSchemaElement("interval"), new Interval(3, 2, 1) },

         new object[] {  new SchemaElement<byte>("byte"), byte.MinValue },
         new object[] {  new SchemaElement<byte>("byte"), byte.MaxValue },
         new object[] {  new SchemaElement<sbyte>("sbyte"), sbyte.MinValue },
         new object[] {  new SchemaElement<sbyte>("sbyte"), sbyte.MaxValue },

         new object[] {  new SchemaElement<short>("short"), short.MinValue },
         new object[] {  new SchemaElement<short>("short"), short.MaxValue },
         new object[] {  new SchemaElement<ushort>("ushort"), ushort.MinValue },
         new object[] {  new SchemaElement<ushort>("ushort"), ushort.MaxValue }
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
