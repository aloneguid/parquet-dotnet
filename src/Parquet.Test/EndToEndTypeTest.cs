using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;
using Xunit.Extensions;

namespace Parquet.Test
{
   public class EndToEndTypeTest
   {
      public static IEnumerable<object[]> TypeData => new[]
      {
         new object[] {  new SchemaElement<string>("s"), "plain string" },
         new object[] {  new SchemaElement<string>("s"), "L'Oréal Paris" },
         new object[] {  new SchemaElement<float>("f"), (float)1.23 },
         new object[] {  new SchemaElement<double>("d"), (double)10.44 },

         //loses precision slightly, i.e.
         //Expected: 2017-07-13T10:58:44.3767154+00:00
         //Actual:   2017-07-12T10:58:44.3770000+00:00
         new object[] {  new SchemaElement<DateTimeOffset>("d"), new DateTimeOffset(DateTime.UtcNow.RoundToSecond()) },
         new object[] {  new DateTimeSchemaElement("d", DateTimeFormat.Impala), new DateTimeOffset(DateTime.UtcNow.RoundToSecond()) },

         new object[] {  new DateTimeSchemaElement("d", DateTimeFormat.DateAndTime), new DateTimeOffset(DateTime.UtcNow.RoundToSecond()) },
         // don't want any excess info in the offset INT32 doesn't contain or care about this data 
         new object[] {  new DateTimeSchemaElement("d", DateTimeFormat.Date), new DateTimeOffset(DateTime.UtcNow.RoundToDay(), TimeSpan.Zero) }
      };

      [Theory]
      [MemberData(nameof(TypeData))]
      public void Type_writes_and_reads_end_to_end(SchemaElement schema, object value)
      {
         var ds = new DataSet(schema) { new Row(value) };
         var ms = new MemoryStream();
         ParquetWriter.Write(ds, ms);

         ms.Position = 0;
         DataSet ds1 = ParquetReader.Read(ms);

         Assert.Equal(ds[0][0], ds1[0][0]);
      }
   }
}
