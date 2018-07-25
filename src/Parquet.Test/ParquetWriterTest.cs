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
            using (ParquetRowGroupWriter gw = writer.CreateRowGroup(1))
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
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup(1))
            {
               rg.WriteColumn(new DataColumn(id, new int[] { 1 }));
            }

            using (ParquetRowGroupWriter rg = writer.CreateRowGroup(1))
            {
               rg.WriteColumn(new DataColumn(id, new int[] { 2 }));
            }

            using (ParquetRowGroupWriter rg = writer.CreateRowGroup(1))
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
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup(2))
            {
               rg.WriteColumn(new DataColumn(id, new int[] { 1, 2 }));
            }
         }

         //append to this file. Note that you cannot append to existing row group, therefore create a new one
         ms.Position = 0;
         using (var writer = new ParquetWriter(new Schema(id), ms, append: true))
         {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup(2))
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

      /*[Fact]
      public void Append_to_file_with_different_schema_fails()
      {
         var ms = new MemoryStream();

         var ds1 = new DataSet(new DataField<int>("id"));
         ds1.Add(1);
         ds1.Add(2);
         ParquetWriter2.Write(ds1, ms);

         //append to file
         var ds2 = new DataSet(new DataField<double>("id"));
         ds2.Add(3d);
         ds2.Add(4d);
         Assert.Throws<ParquetException>(() => ParquetWriter2.Write(ds2, ms, CompressionMethod.Gzip, null, null, true));
      }*/

      /*[Fact]
      public void Writing_another_chunk_validates_schema()
      {

         var ds1 = new DataSet(new DataField<int>("id"));
         var ds2 = new DataSet(new DataField<int>("id1"));

         using (var ms = new MemoryStream())
         {
            using (var ps = new ParquetWriter2(ms))
            {
               ps.Write(ds1);

               Assert.Throws<ParquetException>(() => ps.Write(ds2));
            }
         }
      }*/

      /*[Fact]
      public void Writing_no_values_is_possible()
      {
         var ds = new DataSet(new DataField("id", DataType.Int32));

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal(0, ds1.TotalRowCount);
         Assert.Equal(0, ds1.RowCount);
      }*/

      /*[Fact]
      public void Write_negative_decimals_in_legacy_mode()
      {
         var ds = new DataSet(new DecimalDataField("dec", 10, 2, true));
         ds.Add(-1.0m);
         ds.Add(-1.23m);
         ds.Add(-23233.12m);
         ds.Add(-999999.999m);

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal(         -1.0m, (decimal)(ds1[0][0]), 2);
         Assert.Equal(        -1.23m, (decimal)(ds1[1][0]), 2);
         Assert.Equal(    -23233.12m, (decimal)(ds1[2][0]), 2);
         Assert.Equal(  -999999.99m, (decimal)(ds1[3][0]), 2);
      }*/
   }
}