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

      /*[Fact]
      public void Write_in_small_row_groups()
      {
         var options = new WriterOptions { RowGroupsSize = 5 };

         var ds = new DataSet(new DataField<int>("index"));
         for(int i = 0; i < 103; i++)
         {
            ds.Add(new Row(i));
         }

         var ms = new MemoryStream();
         ParquetWriter2.Write(ds, ms, CompressionMethod.None, null, options);

         ms.Position = 0;
         DataSet ds1 = ParquetReader2.Read(ms);
         Assert.Equal(1, ds1.FieldCount);
         Assert.Equal(103, ds1.RowCount);
      }*/

      /*[Fact]
      public void Append_to_file_reads_all_dataset()
      {
         var ms = new MemoryStream();

         var ds1 = new DataSet(new DataField<int>("id"));
         ds1.Add(1);
         ds1.Add(2);
         ParquetWriter2.Write(ds1, ms);

         //append to file
         var ds2 = new DataSet(new DataField<int>("id"));
         ds2.Add(3);
         ds2.Add(4);
         ParquetWriter2.Write(ds2, ms, CompressionMethod.Gzip, null, null, true);

         ms.Position = 0;
         DataSet dsAll = ParquetReader2.Read(ms);

         Assert.Equal(4, dsAll.RowCount);
         Assert.Equal(new[] {1, 2, 3, 4}, dsAll.GetColumn((DataField)ds1.Schema[0]));
      }*/


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