using NetBox.IO;
using Parquet.Data;
using System;
using System.IO;
using Xunit;
using NetBox.Extensions;
using NetBox.Generator;

namespace Parquet.Test
{
   public class ParquetReaderTest : TestBase
   {
      [Fact]
      public void Opening_null_stream_fails()
      {
         Assert.Throws<ArgumentNullException>(() => new ParquetReader(null));
      }

      [Fact]
      public void Opening_small_file_fails()
      {
         Assert.Throws<IOException>(() => new ParquetReader("small".ToMemoryStream()));
      }

      [Fact]
      public void Opening_file_without_proper_head_fails()
      {
         Assert.Throws<IOException>(() => new ParquetReader("PAR2dataPAR1".ToMemoryStream()));
      }

      [Fact]
      public void Opening_file_without_proper_tail_fails()
      {
         Assert.Throws<IOException>(() => new ParquetReader("PAR1dataPAR2".ToMemoryStream()));
      }

      [Fact]
      public void Opening_readable_but_not_seekable_stream_fails()
      {
         Assert.Throws<ArgumentException>(() => new ParquetReader(new ReadableNonSeekableStream(new MemoryStream(RandomGenerator.GetRandomBytes(5, 6)))));
      }

      [Fact]
      public void Opening_not_readable_but_seekable_stream_fails()
      {
         Assert.Throws<ArgumentException>(() => new ParquetReader(new NonReadableSeekableStream(new MemoryStream(RandomGenerator.GetRandomBytes(5, 6)))));
      }

      

      [Fact]
      public void Read_simple_list_with_one_item()
      {
         /*
root
|-- cities: array (nullable = true)
|    |-- element: string (containsNull = true)
|-- id: long (nullable = true)
          */

         /*DataSet ds = ParquetReader2.Read(OpenTestFile("simplerepeated.parquet"));

         Assert.Equal(2, ds.Schema.Length);
         Assert.Equal(SchemaType.List, ds.Schema[0].SchemaType);
         Assert.Equal(SchemaType.Data, ds.Schema[1].SchemaType);

         Assert.Equal("cities", ds.Schema[0].Name);
         Assert.Equal("id", ds.Schema[1].Name);

         Assert.Equal(ds[0][0], new[] { "London", "Derby", "Paris", "New York" });
         Assert.Equal(1L, ds[0][1]);

         Assert.Equal("{[London;Derby;Paris;New York];1}", ds[0].ToString());*/
      }

      [Fact]
      public void Read_simple_structure()
      {
         /*
          * root
|-- city: struct (nullable = true)
|    |-- country: string (nullable = true)
|    |-- isCapital: boolean (nullable = true)
|    |-- name: string (nullable = true)
|-- id: long (nullable = true)
          */

         //Assert.Throws<NotSupportedException>(() => ParquetReader.Read(OpenTestFile("simplenested.parquet")));
         //return;

        /* DataSet ds = ParquetReader2.Read(OpenTestFile("simplenested.parquet"));

         Assert.Equal(1, ds.RowCount);
         Assert.Equal(2, ds.FieldCount);

         Assert.Equal(SchemaType.Struct, ds.Schema[0].SchemaType);
         Assert.Equal(SchemaType.Data, ds.Schema[1].SchemaType);

         Assert.Equal("city", ds.Schema.FieldNames[0]);
         Assert.Equal("id", ds.Schema.FieldNames[1]);

         Row mr = ds[0];

         Row city = mr.Get<Row>(0);
         Assert.Equal("United Kingdom", city[0]);
         Assert.True((bool)city[1]);
         Assert.Equal("London", city[2]);

         Assert.Equal(1L, mr[1]);
         Assert.Equal("{{United Kingdom;True;London};1}", ds[0].ToString());*/
      }

      [Fact]
      public void Read_simple_map()
      {
         using (var reader = new ParquetReader(OpenTestFile("map_simple.parquet"), leaveStreamOpen: false))
         {
            DataColumn[] data = reader.ReadEntireRowGroup();

            Assert.Equal(new int?[] { 1 }, data[0].Data);
            Assert.Equal(new int[] { 1, 2, 3 }, data[1].Data);
            Assert.Equal(new string[] { "one", "two", "three" }, data[2].Data);
         }
      }

      [Fact]
      public void Read_hardcoded_decimal()
      {
         using (var reader = new ParquetReader(OpenTestFile("complex-primitives.parquet")))
         {
            decimal value = (decimal)reader.ReadEntireRowGroup()[1].Data.GetValue(0);
            Assert.Equal((decimal)1.2, value);
         }
      }


      [Fact]
      public void Reads_multi_page_file()
      {
         using (var reader = new ParquetReader(OpenTestFile("multi.page.parquet"), leaveStreamOpen: false))
         {
            DataColumn[] data = reader.ReadEntireRowGroup();
            Assert.Equal(927861, data[0].Data.Length);

            int[] firstColumn = (int[])data[0].Data;
            Assert.Equal(30763, firstColumn[524286]);
            Assert.Equal(30766, firstColumn[524287]);

            //At row 524288 the data is split into another page
            //The column makes use of a dictionary to reduce the number of values and the default dictionary index value is zero (i.e. the first record value)
            Assert.NotEqual(firstColumn[0], firstColumn[524288]);

            //The vlaue should be 30768
            Assert.Equal(30768, firstColumn[524288]);
         }
      }

      class ReadableNonSeekableStream : DelegatedStream
      {
         public ReadableNonSeekableStream(Stream master) : base(master)
         {
         }

         public override bool CanSeek => false;

         public override bool CanRead => true;
      }

      class NonReadableSeekableStream : DelegatedStream
      {
         public NonReadableSeekableStream(Stream master) : base(master)
         {
         }

         public override bool CanSeek => true;

         public override bool CanRead => false;
      }

      class ReadableAndSeekableStream : DelegatedStream
      {
         public ReadableAndSeekableStream(Stream master) : base(master)
         {
         }

         public override bool CanSeek => true;

         public override bool CanRead => true;

      }
   }
}
