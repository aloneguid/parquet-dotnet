using NetBox.IO;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using Xunit;
using NetBox.Extensions;
using NetBox.Generator;
using Parquet.Data.Rows;
using System.Linq;

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

            //The value should be 30768
            Assert.Equal(30768, firstColumn[524288]);
         }
      }

      [Fact]
      public void Reads_byte_arrays()
      {
         byte[] nameValue;
         byte[] expectedValue = Encoding.UTF8.GetBytes("ALGERIA");
         using (var reader = new ParquetReader(OpenTestFile(@"real/nation.plain.parquet"), leaveStreamOpen: false))
         {
            DataColumn[] data = reader.ReadEntireRowGroup();

            byte[][] nameColumn = (byte[][]) data[1].Data;
            nameValue = nameColumn[0];
            Assert.Equal<IEnumerable<byte>>(expectedValue, nameValue);

         }
         Assert.Equal<IEnumerable<byte>>(expectedValue, nameValue);
      }

      [Fact]
      public void Read_multiple_data_pages()
      {
         using (var reader =
            new ParquetReader(OpenTestFile("/special/multi_data_page.parquet"), leaveStreamOpen: false))
         {
            DataColumn[] columns = reader.ReadEntireRowGroup();

            string[] s = (string[]) columns[0].Data;
            double?[] d = (double?[]) columns[1].Data;

            // check for nulls (issue #370)
            for (int i = 0; i < s.Length; i++)
            {
               Assert.True(s[i] != null, "found null in s at " + i);
               Assert.True(d[i] != null, "found null in d at " + i);
            }

            // run aggregations checking row alignment (issue #371)
            var seq = s.Zip(d.Cast<double>(), (w, v) => new {w, v})
               .Where(p => p.w == "general")
               .ToList();

            // double matching is fuzzy, but matching strings is enough for this test
            Assert.Equal("0.754359925788497", seq.Min(p => p.v).ToString(CultureInfo.InvariantCulture));
            Assert.Equal("0.85776", seq.Max(p => p.v).ToString(CultureInfo.InvariantCulture));
         }
      }

      [Fact]
      public void ReadLargeTimestampData()
      {
         using (var reader = new ParquetReader(OpenTestFile("/mixed-dictionary-plain.parquet"), leaveStreamOpen: false))
         {
            DataColumn[] columns = reader.ReadEntireRowGroup();

            DateTimeOffset?[] col0 = (DateTimeOffset?[])columns[0].Data;
            Assert.Equal(440773, col0.Length);

            long ticks = col0[0].Value.Ticks;
            for (int i = 1; i < 132000; i++)
            {
               long now = col0[i].Value.Ticks;
               Assert.NotEqual(ticks, now);
            }
         }
      }

      [Fact]
      public void ParquetReader_OpenFromFile_Close_Stream()
      {
         // copy a file to a temp location
         var tempFile = Path.GetTempFileName();
         using (var fr = OpenTestFile("map_simple.parquet"))
         using (var fw = System.IO.File.OpenWrite(tempFile))
         {
            fr.CopyTo(fw);
         }
               
         // open the copy
         using (var reader = ParquetReader.OpenFromFile(tempFile))
         {
            // do nothing
         }
         
         // now try to delete this temp file. If the stream is properly closed, this should succeed
         System.IO.File.Delete(tempFile);
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
