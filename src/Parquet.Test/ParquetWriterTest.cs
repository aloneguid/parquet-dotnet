using System;
using Parquet.Data;
using System.IO;
using Xunit;
using F = System.IO.File;

namespace Parquet.Test
{
   public class ParquetWriterTest
   {
      [Fact]
      public void Write_different_compressions()
      {
         var ds = new DataSet(
            new SchemaElement<int>("id", false),
            new SchemaElement<bool>("bool_col", false),
            new SchemaElement<string>("string_col", false)
         );

         //8 values for each column

         ds.Add(4, true, "0");
         ds.Add(5, false, "1");
         ds.Add(6, true, "0");
         ds.Add(7, false, "1");
         ds.Add(2, true, "0");
         ds.Add(3, false, "1");
         ds.Add(0, true, "0");
         ds.Add(1, false, "0");

         var uncompressed = new MemoryStream();
         using (var writer = new ParquetWriter(uncompressed))
         {
            writer.Write(ds, CompressionMethod.None);
         }

         var compressed = new MemoryStream();
         using (var writer = new ParquetWriter(compressed))
         {
            writer.Write(ds, CompressionMethod.Gzip);
         }

#if DEBUG
            const string path = "c:\\tmp\\first.parquet";
         F.WriteAllBytes(path, uncompressed.ToArray());
#endif

      }

      [Fact]
      public void Write_datetimeoffset()
      {
         var ds = new DataSet(
            new SchemaElement<DateTimeOffset>("timestamp_col", false)
         )
         {
            new DateTimeOffset(new DateTime(2017, 1, 1, 12, 13, 22)),
            new DateTimeOffset(new DateTime(2017, 1, 1, 12, 13, 23))
         };

         //8 values for each column


         var uncompressed = new MemoryStream();
         using (var writer = new ParquetWriter(uncompressed))
         {
            writer.Write(ds, CompressionMethod.None);
         }

#if DEBUG
         const string path = "c:\\tmp\\first.parquet";
         F.WriteAllBytes(path, uncompressed.ToArray());
#endif

      }

      //[Fact]
      public void Delete_me()
      {
         DataSet ds = ParquetReader.ReadFile("c:\\tmp\\test.parquet");
      }

      //[Fact]
      public void Write_large_gzip()
      {
         var ds = new DataSet(
            new SchemaElement<string>("Postcode", false)
            );

         for(int i = 0; i < 10000; i++)
         {
            ds.Add("postcode #" + i);
         }

         var ms = new MemoryStream();
         using (var writer = new ParquetWriter(ms))
         {
            writer.Write(ds, CompressionMethod.Gzip);
         }

#if DEBUG
         const string path = "c:\\tmp\\firstgzip.parquet";
         F.WriteAllBytes(path, ms.ToArray());
#endif

      }
   }
}
