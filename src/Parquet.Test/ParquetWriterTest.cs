using System;
using Parquet.Data;
using System.IO;
using Xunit;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Parquet.Test
{
   public class ParquetWriterTest : TestBase
   {
      [Fact]
      public async Task Cannot_write_columns_in_wrong_orderAsync()
      {
         var schema = new Schema(new DataField<int>("id"), new DataField<int>("id2"));

         await using (ParquetWriter writer = await ParquetWriter.CreateParquetWriterAsync(schema, new MemoryStream()))
         {

            using (ParquetRowGroupWriter gw = writer.CreateRowGroup())
            {
               await Assert.ThrowsAsync<ArgumentException>(() =>
                  gw.WriteColumnAsync(new DataColumn((DataField)schema[1], new int[] { 1 }))
               ).ConfigureAwait(false);
            }
         }
      }

      [Fact]
      public async Task Write_in_small_row_groupsAsync()
      {
         //write a single file having 3 row groups
         var id = new DataField<int>("id");
         var ms = new MemoryStream();

         await using (ParquetWriter writer = await ParquetWriter.CreateParquetWriterAsync(new Schema(id), ms))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               await rg.WriteColumnAsync(new DataColumn(id, new int[] { 1 })).ConfigureAwait(false);
            }

            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               await rg.WriteColumnAsync(new DataColumn(id, new int[] { 2 })).ConfigureAwait(false);
            }

            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               await rg.WriteColumnAsync(new DataColumn(id, new int[] { 3 })).ConfigureAwait(false);
            }

         }

         //read the file back and validate
         ms.Position = 0;
         await using (ParquetReader reader = await ParquetReader.OpenFromStreamAsync(ms))
         {
            Assert.Equal(3, reader.RowGroupCount);

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(0))
            {
               Assert.Equal(1, rg.RowCount);
               DataColumn dc = await rg.ReadColumnAsync(id).ConfigureAwait(false);
               Assert.Equal(new int[] { 1 }, dc.Data);
            }

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(1))
            {
               Assert.Equal(1, rg.RowCount);
               DataColumn dc = await rg.ReadColumnAsync(id).ConfigureAwait(false);
               Assert.Equal(new int[] { 2 }, dc.Data);
            }

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(2))
            {
               Assert.Equal(1, rg.RowCount);
               DataColumn dc = await rg.ReadColumnAsync(id).ConfigureAwait(false);
               Assert.Equal(new int[] { 3 }, dc.Data);
            }
         }
      }

      [Fact]
      public async Task Append_to_file_reads_all_dataAsync()
      {
         //write a file with a single row group
         var id = new DataField<int>("id");
         var ms = new MemoryStream();

         await using (ParquetWriter writer = await ParquetWriter.CreateParquetWriterAsync(new Schema(id), ms))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               await rg.WriteColumnAsync(new DataColumn(id, new int[] { 1, 2 })).ConfigureAwait(false);
            }
         }

         //append to this file. Note that you cannot append to existing row group, therefore create a new one
         ms.Position = 0;
         await using (ParquetWriter writer = await ParquetWriter.CreateParquetWriterAsync(new Schema(id), ms, append: true))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               await rg.WriteColumnAsync(new DataColumn(id, new int[] { 3, 4 })).ConfigureAwait(false);
            }
         }

         //check that this file now contains two row groups and all the data is valid
         ms.Position = 0;
         await using (ParquetReader reader = await ParquetReader.OpenFromStreamAsync(ms))
         {
            Assert.Equal(2, reader.RowGroupCount);

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(0))
            {
               Assert.Equal(2, rg.RowCount);
               Assert.Equal(new int[] { 1, 2 }, (await rg.ReadColumnAsync(id).ConfigureAwait(false)).Data);
            }

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(1))
            {
               Assert.Equal(2, rg.RowCount);
               Assert.Equal(new int[] { 3, 4 }, (await rg.ReadColumnAsync(id).ConfigureAwait(false)).Data);
            }

         }
      }
      
      public readonly static IEnumerable<object[]> NullableColumnContentCases = new List<object[]>()
      {
         new object[] { new int?[] { 1, 2 } },
         new object[] { new int?[] { null } },
         new object[] { new int?[] { 1, null, 2 } },
         new object[] { new int[] { 1, 2 } },
      };

      [Theory]
      [MemberData(nameof(NullableColumnContentCases))]
      public async Task Write_read_nullable_columnAsync(Array input)
      {
         var id = new DataField<int?>("id");
         var ms = new MemoryStream();

         await using (ParquetWriter writer = await ParquetWriter.CreateParquetWriterAsync(new Schema(id), ms))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               await rg.WriteColumnAsync(new DataColumn(id, input)).ConfigureAwait(false);
            }
         }

         ms.Position = 0;
         await using (ParquetReader reader = await ParquetReader.OpenFromStreamAsync(ms))
         {
            Assert.Equal(1, reader.RowGroupCount);

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(0))
            {
               Assert.Equal(input.Length, rg.RowCount);
               Assert.Equal(input, (await rg.ReadColumnAsync(id).ConfigureAwait(false)).Data);
            }
         }
      }

      [Fact]
      public async Task FileMetadata_sets_num_rows_on_file_and_row_groupAsync()
      {
         var ms = new MemoryStream();
         var id = new DataField<int>("id");

         //write
         await using (ParquetWriter writer = await ParquetWriter.CreateParquetWriterAsync(new Schema(id), ms))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               await rg.WriteColumnAsync(new DataColumn(id, new[] { 1, 2, 3, 4 })).ConfigureAwait(false);
            }
         }

         //read back
         await using (ParquetReader reader = await ParquetReader.OpenFromStreamAsync(ms))
         {
            Assert.Equal(4, reader.ThriftMetadata.Num_rows);

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(0))
            {
               Assert.Equal(4, rg.RowCount);
            }
         }
      }

      [Fact]
      public async Task FileMetadata_sets_num_rows_on_file_and_row_group_multiple_row_groupsAsync()
      {
         var ms = new MemoryStream();
         var id = new DataField<int>("id");

         //write
         await using (ParquetWriter writer = await ParquetWriter.CreateParquetWriterAsync(new Schema(id), ms))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               await rg.WriteColumnAsync(new DataColumn(id, new[] { 1, 2, 3, 4 })).ConfigureAwait(false);
            }

            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               await rg.WriteColumnAsync(new DataColumn(id, new[] { 5, 6 })).ConfigureAwait(false);
            }
         }

         //read back
         await using (ParquetReader reader = await ParquetReader.OpenFromStreamAsync(ms))
         {
            Assert.Equal(6, reader.ThriftMetadata.Num_rows);

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(0))
            {
               Assert.Equal(4, rg.RowCount);
            }

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(1))
            {
               Assert.Equal(2, rg.RowCount);
            }
         }
      }

      [Fact]
      public async Task CustomMetadata_can_write_and_readAsync()
      {
         var ms = new MemoryStream();
         var id = new DataField<int>("id");

         //write
         await using (ParquetWriter writer = await ParquetWriter.CreateParquetWriterAsync(new Schema(id), ms))
         {
            writer.CustomMetadata = new Dictionary<string, string>
            {
               ["key1"] = "value1",
               ["key2"] = "value2"
            };

            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               await rg.WriteColumnAsync(new DataColumn(id, new[] { 1, 2, 3, 4 })).ConfigureAwait(false);
            }
         }

         //read back
         await using (ParquetReader reader = await ParquetReader.OpenFromStreamAsync(ms))
         {
            Assert.Equal("value1", reader.CustomMetadata["key1"]);
            Assert.Equal("value2", reader.CustomMetadata["key2"]);
         }
      }
   }
}