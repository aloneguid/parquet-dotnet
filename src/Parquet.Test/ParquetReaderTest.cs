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

      //[Fact]
      public void Issue()
      {
         ParquetReader.ReadTableFromFile("c:\\tmp\\a.parquet");
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
