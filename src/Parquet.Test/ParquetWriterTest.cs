using System;
using Parquet.Data;
using System.IO;
using Parquet.Thrift;
using Xunit;
using F = System.IO.File;
using Type = Parquet.Thrift.Type;

namespace Parquet.Test
{
   public class ParquetWriterTest
   {
      [Fact]
      public void Write_different_compressions()
      {
         var ds = new DataSet(
            new SchemaElement<int>("id"),
            new SchemaElement<bool>("bool_col"),
            new SchemaElement<string>("string_col")
         )
         {
            //8 values for each column

            { 4, true, "0" },
            { 5, false, "1" },
            { 6, true, "0" },
            { 7, false, "1" },
            { 2, true, "0" },
            { 3, false, "1" },
            { 0, true, "0" },
            { 1, false, "0" }
         };
         var uncompressed = new MemoryStream();
         ParquetWriter.Write(ds, uncompressed, CompressionMethod.None);

         var compressed = new MemoryStream();
         ParquetWriter.Write(ds, compressed, CompressionMethod.Gzip);

         var compressedSnappy = new MemoryStream();
         ParquetWriter.Write(ds, compressedSnappy, CompressionMethod.Snappy);
      }

      [Fact]
      public void Write_int64datetimeoffset()
      {
         var element = new SchemaElement<DateTimeOffset>("timestamp_col");
         /*{
            ThriftConvertedType = ConvertedType.TIMESTAMP_MILLIS,
            ThriftOriginalType = Type.INT64
         };*/

         var ds = new DataSet(
            element  
         )
         {
            new DateTimeOffset(new DateTime(2017, 1, 1, 12, 13, 22)),
            new DateTimeOffset(new DateTime(2017, 1, 1, 12, 13, 24))
         };

         //8 values for each column


         var uncompressed = new MemoryStream();
         using (var writer = new ParquetWriter(uncompressed))
         {
            writer.Write(ds, CompressionMethod.None);
         }
      }

      [Fact]
      public void Write_and_read_nullable_integers()
      {
         var ds = new DataSet(new SchemaElement<int>("id"))
         {
            1,
            2,
            3,
            (object)null,
            4,
            (object)null,
            5
         };
         var ms = new MemoryStream();
         ParquetWriter.Write(ds, ms);

         ms.Position = 0;
         DataSet ds1 = ParquetReader.Read(ms);

         Assert.Equal(ds1[0].GetInt(0), 1);
         Assert.Equal(ds1[1].GetInt(0), 2);
         Assert.Equal(ds1[2].GetInt(0), 3);
         Assert.True(ds1[3].IsNullAt(0));
         Assert.Equal(ds1[4].GetInt(0), 4);
         Assert.True(ds1[5].IsNullAt(0));
         Assert.Equal(ds1[6].GetInt(0), 5);
      }
   }
}
