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
      public void Write_different_compressions()
      {
         var ds = new DataSet(
            new DataField<int>("id"),
            new DataField<bool>("bool_col"),
            new DataField<string>("string_col")
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
         var element = new DataField<DateTimeOffset>("timestamp_col");

         var ds = new DataSet(
            element
         )
         {
            new DateTimeOffset(new DateTime(2017, 1, 1, 12, 13, 22)),
            new DateTimeOffset(new DateTime(2017, 1, 1, 12, 13, 24))
         };

         var uncompressed = new MemoryStream();
         using (var writer = new ParquetWriter(uncompressed))
         {
            writer.Write(ds, CompressionMethod.None);
         }
      }

      [Fact]
      public void Write_and_read_nullable_integers()
      {
         var ds = new DataSet(new DataField<int?>("id"))
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

         Assert.Equal(1, ds1[0].GetInt(0));
         Assert.Equal(2, ds1[1].GetInt(0));
         Assert.Equal(3, ds1[2].GetInt(0));
         Assert.True(ds1[3].IsNullAt(0));
         Assert.Equal(4, ds1[4].GetInt(0));
         Assert.True(ds1[5].IsNullAt(0));
         Assert.Equal(5, ds1[6].GetInt(0));
      }

      [Fact]
      public void Write_in_small_row_groups()
      {
         var options = new WriterOptions { RowGroupsSize = 5 };

         var ds = new DataSet(new DataField<int>("index"));
         for(int i = 0; i < 103; i++)
         {
            ds.Add(new Row(i));
         }

         var ms = new MemoryStream();
         ParquetWriter.Write(ds, ms, CompressionMethod.None, null, options);

         ms.Position = 0;
         DataSet ds1 = ParquetReader.Read(ms);
         Assert.Equal(1, ds1.FieldCount);
         Assert.Equal(103, ds1.RowCount);
      }

      [Fact]
      public void Write_supposably_in_dictionary_encoding()
      {
         var ds = new DataSet(new DataField<int>("id"), new DataField<string>("dic_col"));
         ds.Add(1, "one");
         ds.Add(2, "one");
         ds.Add(3, "one");
         ds.Add(4, "one");
         ds.Add(5, "one");
         ds.Add(6, "two");
         ds.Add(7, "two");

         ds = DataSetGenerator.WriteRead(ds);


      }

      [Fact]
      public void Append_to_file_reads_all_dataset()
      {
         var ms = new MemoryStream();

         var ds1 = new DataSet(new DataField<int>("id"));
         ds1.Add(1);
         ds1.Add(2);
         ParquetWriter.Write(ds1, ms);

         //append to file
         var ds2 = new DataSet(new DataField<int>("id"));
         ds2.Add(3);
         ds2.Add(4);
         ParquetWriter.Write(ds2, ms, CompressionMethod.Gzip, null, null, true);

         ms.Position = 0;
         DataSet dsAll = ParquetReader.Read(ms);

         Assert.Equal(4, dsAll.RowCount);
         Assert.Equal(new[] {1, 2, 3, 4}, dsAll.GetColumn((DataField)ds1.Schema[0]));
      }

      [Fact]
      public void Append_to_file_works_for_all_data_types()
      {
         var ms = new MemoryStream();

         var schema = new Schema(
            new DataField<int>("Id"),
            new DataField<DateTime>("Timestamp"),
            new DataField<DateTimeOffset>("Timestamp2"),
            new DataField<string>("Message"),
            new DataField<byte[]>("Data"),
            new DataField<bool>("IsDeleted"),
            new DataField<float>("Amount"),
            new DataField<decimal>("TotalAmount"),
            new DataField<long>("Counter"),
            new DataField<double>("Amount2"),
            new DataField<byte>("Flag"),
            new DataField<sbyte>("Flag2"),
            new DataField<short>("Flag3"),
            new DataField<ushort>("Flag4"),
            new MapField("MapData", new DataField<string>("Key"),  new DataField<string>("Value")));

         var ds1 = new DataSet(schema);
         var dict = new Dictionary<string, string> { { "foo", "bar" } };
         ds1.Add(1, DateTime.Now, DateTimeOffset.Now, "Record1", System.Text.Encoding.ASCII.GetBytes("SomeData"), false, 123.4f, 200M, 100000L, 1331313D, (byte)1, (sbyte)-1, (short)-500, (ushort)500, dict);
         ds1.Add(1, DateTime.Now, DateTimeOffset.Now, "Record2", System.Text.Encoding.ASCII.GetBytes("SomeData2"), false, 124.4f, 300M, 200000L, 2331313D, (byte)2, (sbyte)-2, (short)-400, (ushort)400, dict);

         ParquetWriter.Write(ds1, ms, CompressionMethod.Snappy, null, null, false);

         var ds2 = new DataSet(schema);
         ds2.Add(1, DateTime.Now, DateTimeOffset.Now, "Record3", System.Text.Encoding.ASCII.GetBytes("SomeData3"), false, 125.4f, 400M, 300000L, 3331313D, (byte)3, (sbyte)-3, (short)-600, (ushort)600, dict);
         ds2.Add(1, DateTime.Now, DateTimeOffset.Now, "Record4", System.Text.Encoding.ASCII.GetBytes("SomeData4"), false, 126.4f, 500M, 400000L, 4331313D, (byte)4, (sbyte)-4, (short)-700, (ushort)700, dict);

         ParquetWriter.Write(ds2, ms, CompressionMethod.Snappy, null, null, true);
      }

      [Fact]
      public void Append_to_file_with_different_schema_fails()
      {
         var ms = new MemoryStream();

         var ds1 = new DataSet(new DataField<int>("id"));
         ds1.Add(1);
         ds1.Add(2);
         ParquetWriter.Write(ds1, ms);

         //append to file
         var ds2 = new DataSet(new DataField<double>("id"));
         ds2.Add(3d);
         ds2.Add(4d);
         Assert.Throws<ParquetException>(() => ParquetWriter.Write(ds2, ms, CompressionMethod.Gzip, null, null, true));
      }

      [Fact]
      public void Write_column_with_only_one_null_value()
      {
         var ds = new DataSet(
           new DataField<int>("id"),
           new DataField<int?>("city")
       );

         ds.Add(0, null);

         var ms = new MemoryStream();
         ParquetWriter.Write(ds, ms);

         ms.Position = 0;
         DataSet ds1 = ParquetReader.Read(ms);

         Assert.Equal(1, ds1.RowCount);
         Assert.Equal(0, ds1[0][0]);
         Assert.Null(ds1[0][1]);
      }

      [Fact]
      public void Writing_another_chunk_validates_schema()
      {

         var ds1 = new DataSet(new DataField<int>("id"));
         var ds2 = new DataSet(new DataField<int>("id1"));

         using (var ms = new MemoryStream())
         {
            using (var ps = new ParquetWriter(ms))
            {
               ps.Write(ds1);

               Assert.Throws<ParquetException>(() => ps.Write(ds2));
            }
         }
      }

      [Fact]
      public void Writing_no_values_is_possible()
      {
         var ds = new DataSet(new DataField("id", DataType.Int32));

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal(0, ds1.TotalRowCount);
         Assert.Equal(0, ds1.RowCount);
      }

      [Fact]
      public void Datetime_as_null_writes()
      {
         var schemaElements = new List<Field>();
         schemaElements.Add(new DataField<string>("primary-key"));
         schemaElements.Add(new DataField<DateTime?>("as-at-date"));

         var ds = new DataSet(schemaElements);

         // row 1
         var row1 = new List<object>(schemaElements.Count);
         row1.Add(Guid.NewGuid().ToString());
         row1.Add(DateTime.UtcNow.AddDays(-5));
         ds.Add(new Row(row1));

         // row 2
         var row2 = new List<object>(schemaElements.Count);
         row2.Add(Guid.NewGuid().ToString());
         row2.Add(DateTime.UtcNow);
         ds.Add(new Row(row2));

         // row 3
         var row3 = new List<object>(schemaElements.Count);
         row3.Add(Guid.NewGuid().ToString());
         row3.Add(null);
         //objData3.Add(DateTime.UtcNow);
         ds.Add(new Row(row3));

         DataSet dsRead = DataSetGenerator.WriteRead(ds);

         Assert.Equal(3, dsRead.RowCount);
      }

      [Fact]
      public void Column_with_all_null_decimals_has_type_length()
      {
         var ds = new DataSet(new DataField<int>("id"), new DataField<decimal?>("nulls"))
         {
            { 1, null },
            { 2, null }
         };

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Null(ds1[0][1]);
         Assert.Null(ds1[1][1]);
      }

      [Fact]
      public void Simplest_write_read()
      {
         var ds = new DataSet(new DataField<int>("id"));
         ds.Add(1);
         ds.Add(2);

         DataSetGenerator.WriteRead(ds);
      }

      [Fact]
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
      }
   }
}
