using System;
using Parquet.Data;
using System.IO;
using Xunit;
using System.Collections.Generic;

namespace Parquet.Test
{
   public class ParquetWriterTest : TestBase
   {
      [Fact]
      public void Cannot_write_columns_in_wrong_order()
      {
         var schema = new Schema(new DataField<int>("id"), new DataField<int>("id2"));

         using (var writer = new ParquetWriter(schema, new MemoryStream()))
         {

            using (ParquetRowGroupWriter gw = writer.CreateRowGroup())
            {
               Assert.Throws<ArgumentException>(() =>
               {
                  gw.WriteColumn(new DataColumn((DataField)schema[1], new int[] { 1 }));
               });
            }
         }
      }

      [Fact]
      public void Write_in_small_row_groups()
      {
         //write a single file having 3 row groups
         var id = new DataField<int>("id");
         var ms = new MemoryStream();

         using (var writer = new ParquetWriter(new Schema(id), ms))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               rg.WriteColumn(new DataColumn(id, new int[] { 1 }));
            }

            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               rg.WriteColumn(new DataColumn(id, new int[] { 2 }));
            }

            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               rg.WriteColumn(new DataColumn(id, new int[] { 3 }));
            }

         }

         //read the file back and validate
         ms.Position = 0;
         using (var reader = new ParquetReader(ms))
         {
            Assert.Equal(3, reader.RowGroupCount);

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(0))
            {
               Assert.Equal(1, rg.RowCount);
               DataColumn dc = rg.ReadColumn(id);
               Assert.Equal(new int[] { 1 }, dc.Data);
            }

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(1))
            {
               Assert.Equal(1, rg.RowCount);
               DataColumn dc = rg.ReadColumn(id);
               Assert.Equal(new int[] { 2 }, dc.Data);
            }

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(2))
            {
               Assert.Equal(1, rg.RowCount);
               DataColumn dc = rg.ReadColumn(id);
               Assert.Equal(new int[] { 3 }, dc.Data);
            }
         }
      }

      [Fact]
      public void Append_to_file_reads_all_data()
      {
         //write a file with a single row group
         var id = new DataField<int>("id");
         var ms = new MemoryStream();

         using (var writer = new ParquetWriter(new Schema(id), ms))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               rg.WriteColumn(new DataColumn(id, new int[] { 1, 2 }));
            }
         }

         //append to this file. Note that you cannot append to existing row group, therefore create a new one
         ms.Position = 0;
         using (var writer = new ParquetWriter(new Schema(id), ms, append: true))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               rg.WriteColumn(new DataColumn(id, new int[] { 3, 4 }));
            }
         }

         //check that this file now contains two row groups and all the data is valid
         ms.Position = 0;
         using (var reader = new ParquetReader(ms))
         {
            Assert.Equal(2, reader.RowGroupCount);

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(0))
            {
               Assert.Equal(2, rg.RowCount);
               Assert.Equal(new int[] { 1, 2 }, rg.ReadColumn(id).Data);
            }

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(1))
            {
               Assert.Equal(2, rg.RowCount);
               Assert.Equal(new int[] { 3, 4 }, rg.ReadColumn(id).Data);
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
      public void Write_read_nullable_column(Array input)
      {
         var id = new DataField<int?>("id");
         var ms = new MemoryStream();

         using (var writer = new ParquetWriter(new Schema(id), ms))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               rg.WriteColumn(new DataColumn(id, input));
            }
         }

         ms.Position = 0;
         using (var reader = new ParquetReader(ms))
         {
            Assert.Equal(1, reader.RowGroupCount);

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(0))
            {
               Assert.Equal(input.Length, rg.RowCount);
               Assert.Equal(input, rg.ReadColumn(id).Data);
            }
         }
      }

      [Fact]
      public void FileMetadata_sets_num_rows_on_file_and_row_group()
      {
         var ms = new MemoryStream();
         var id = new DataField<int>("id");

         //write
         using (var writer = new ParquetWriter(new Schema(id), ms))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               rg.WriteColumn(new DataColumn(id, new[] { 1, 2, 3, 4 }));
            }
         }

         //read back
         using (var reader = new ParquetReader(ms))
         {
            Assert.Equal(4, reader.ThriftMetadata.Num_rows);

            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(0))
            {
               Assert.Equal(4, rg.RowCount);
            }
         }
      }

      [Fact]
      public void FileMetadata_sets_num_rows_on_file_and_row_group_multiple_row_groups()
      {
         var ms = new MemoryStream();
         var id = new DataField<int>("id");

         //write
         using (var writer = new ParquetWriter(new Schema(id), ms))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               rg.WriteColumn(new DataColumn(id, new[] { 1, 2, 3, 4 }));
            }

            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               rg.WriteColumn(new DataColumn(id, new[] { 5, 6 }));
            }
         }

         //read back
         using (var reader = new ParquetReader(ms))
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
      public void CustomMetadata_can_write_and_read()
      {
         var ms = new MemoryStream();
         var id = new DataField<int>("id");

         //write
         using (var writer = new ParquetWriter(new Schema(id), ms))
         {
            writer.CustomMetadata = new Dictionary<string, string>
            {
               ["key1"] = "value1",
               ["key2"] = "value2"
            };

            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               rg.WriteColumn(new DataColumn(id, new[] { 1, 2, 3, 4 }));
            }
         }

         //read back
         using (var reader = new ParquetReader(ms))
         {
            Assert.Equal("value1", reader.CustomMetadata["key1"]);
            Assert.Equal("value2", reader.CustomMetadata["key2"]);
         }
      }
      
      [Fact]
      public void String_allocation_reduction_correct_strings()
      {
         //write a file with a single row group
         var str = new DataField<string>("str");
         var ms = new MemoryStream();
         
         var string1 = "SomeString1";
         var string2 = "SomeString2";
         var string3 = "xomeString1";
         var repeats = 100;
         var data = new string[repeats * 3];
         Array.Fill<string>(data, string1, 0, repeats);
         Array.Fill<string>(data, string2, repeats, repeats);
         Array.Fill<string>(data, string3, repeats * 2, repeats);
               
         using (var writer = new ParquetWriter(new Schema(str), ms))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
               rg.WriteColumn(new DataColumn(str, data));
            }
         }

         //check that this file now contains strings where we expect
         ms.Position = 0;
         using (var reader = new ParquetReader(ms))
         {
            using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(0))
            {
               Assert.Equal(repeats * 3, rg.RowCount);
               var parquetData = (string[])rg.ReadColumn(str).Data;

               Assert.Equal(string1, parquetData[0]);
               Assert.Equal(string1, parquetData[repeats - 1]);
               Assert.True(ReferenceEquals(parquetData[0], parquetData[repeats - 1]));

               Assert.Equal(string2, parquetData[repeats]);
               Assert.Equal(string2, parquetData[repeats * 2 - 1]);
               Assert.True(ReferenceEquals(parquetData[repeats], parquetData[repeats * 2 - 1]));

               Assert.Equal(string3, parquetData[repeats * 2]);
               Assert.Equal(string3, parquetData[repeats * 3 - 1]);               
               Assert.True(ReferenceEquals(parquetData[repeats * 2], parquetData[repeats * 3 - 1]));
            }
         }
      }

   }
}