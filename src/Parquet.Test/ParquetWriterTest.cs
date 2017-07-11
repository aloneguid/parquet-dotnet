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
            new SchemaElement<int>("id"),
            new SchemaElement<bool>("bool_col"),
            new SchemaElement<string>("string_col")
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
            new SchemaElement<DateTimeOffset>("timestamp_col")
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

      [Fact]
      public void Write_and_read_nullable_integers()
      {
         var ds = new DataSet(new SchemaElement<int>("id"));

         ds.Add(1);
         ds.Add(2);
         ds.Add(3);
         ds.Add((object)null);
         ds.Add(4);
         ds.Add((object)null);
         ds.Add(5);

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
