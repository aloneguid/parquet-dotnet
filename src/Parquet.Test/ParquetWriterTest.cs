using System;
using Parquet.Data;
using System.IO;
using Parquet.Thrift;
using Xunit;
using F = System.IO.File;
using Type = Parquet.Thrift.Type;
using NetBox.IO;
using NetBox;
using System.Collections.Generic;

namespace Parquet.Test
{
   public class ParquetWriterTest : TestBase
   {
      [Fact]
      public void Write_different_compressions()
      {
         var ds = new DataSet(
            new SchemaElement<int>("id"),
            new SchemaElement<bool>("bool_col"),
            new SchemaElement<string>("string_col")
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
         var element = new SchemaElement<DateTimeOffset>("timestamp_col");
         /*{
            ThriftConvertedType = ConvertedType.TIMESTAMP_MILLIS,
            ThriftOriginalType = Type.INT64
         };*/

         var ds = new DataSet(
            element  
         )
         {
            new DateTimeOffset(new DateTime(2017, 1, 1, 12, 13, 22)),
            new DateTimeOffset(new DateTime(2017, 1, 1, 12, 13, 24))
         };

         //8 values for each column


         var uncompressed = new MemoryStream();
         using (var writer = new ParquetWriter(uncompressed))
         {
            writer.Write(ds, CompressionMethod.None);
         }
      }

      [Fact]
      public void Write_and_read_nullable_integers()
      {
         var ds = new DataSet(new SchemaElement<int>("id"))
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

         Assert.Equal(ds1[0].GetInt(0), 1);
         Assert.Equal(ds1[1].GetInt(0), 2);
         Assert.Equal(ds1[2].GetInt(0), 3);
         Assert.True(ds1[3].IsNullAt(0));
         Assert.Equal(ds1[4].GetInt(0), 4);
         Assert.True(ds1[5].IsNullAt(0));
         Assert.Equal(ds1[6].GetInt(0), 5);
      }

      [Fact]
      public void Write_in_small_row_groups()
      {
         var options = new WriterOptions { RowGroupsSize = 5 };

         var ds = new DataSet(new SchemaElement<int>("index"));
         for(int i = 0; i < 103; i++)
         {
            ds.Add(new Row(i));
         }

         var ms = new MemoryStream();
         ParquetWriter.Write(ds, ms, CompressionMethod.None, null, options);

         ms.Position = 0;
         DataSet ds1 = ParquetReader.Read(ms);
         Assert.Equal(1, ds1.ColumnCount);
         Assert.Equal(103, ds1.RowCount);
      }

      [Fact]
      public void Write_supposably_in_dictionary_encoding()
      {
         var ds = new DataSet(new SchemaElement<int>("id"), new SchemaElement<string>("dic_col"));
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

         var ds1 = new DataSet(new SchemaElement<int>("id"));
         ds1.Add(1);
         ds1.Add(2);
         ParquetWriter.Write(ds1, ms);

         //append to file
         var ds2 = new DataSet(new SchemaElement<int>("id"));
         ds2.Add(3);
         ds2.Add(4);
         ParquetWriter.Write(ds2, ms, CompressionMethod.Gzip, null, null, true);

         ms.Position = 0;
         DataSet dsAll = ParquetReader.Read(ms);

         Assert.Equal(4, dsAll.RowCount);
         Assert.Equal(new[] {1, 2, 3, 4}, dsAll.GetColumn(0));
      }

      [Fact]
      public void Append_to_file_with_different_schema_fails()
      {
         var ms = new MemoryStream();

         var ds1 = new DataSet(new SchemaElement<int>("id"));
         ds1.Add(1);
         ds1.Add(2);
         ParquetWriter.Write(ds1, ms);

         //append to file
         var ds2 = new DataSet(new SchemaElement<double>("id"));
         ds2.Add(3d);
         ds2.Add(4d);
         Assert.Throws<ParquetException>(() => ParquetWriter.Write(ds2, ms, CompressionMethod.Gzip, null, null, true));
      }

      [Fact]
      public void Write_column_with_only_one_null_value()
      {
         var ds = new DataSet(
           new SchemaElement<int>("id"),
           new SchemaElement<int>("city")
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
      public void Write_simple_repeated_field()
      {
         var ds = new DataSet(
            new SchemaElement<int>("id"),
            new SchemaElement<IEnumerable<string>>("cities"));

         ds.Add(1, new[] { "London", "Derby" });

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal("id", ds.Schema[0].Name);
         Assert.Equal("cities", ds.Schema[1].Name);
      }

      [Fact]
      public void Write_in_small_chunks_to_forward_only_stream()
      {
         var ms = new MemoryStream();
         var forwardOnly = new WriteableNonSeekableStream(ms);

         var ds = new DataSet(
            new SchemaElement<int>("id"),
            new SchemaElement<string>("nonsense"));
         ds.Add(1, Generator.RandomString);

         using (var writer = new ParquetWriter(forwardOnly))
         {
            writer.Write(ds);
            writer.Write(ds);
            writer.Write(ds);
         }

         ms.Position = 0;
         DataSet ds1 = ParquetReader.Read(ms);

         Assert.Equal(3, ds1.RowCount);

      }

      [Fact]
      public void Writing_another_chunk_validates_schema()
      {

         var ds1 = new DataSet(new SchemaElement<int>("id"));
         var ds2 = new DataSet(new SchemaElement<int>("id1"));

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
      public void Datetime_as_null_writes()
      {
         var schemaElements = new List<Data.SchemaElement>();
         schemaElements.Add(new SchemaElement<string>("primary-key"));
         schemaElements.Add(new SchemaElement<DateTime>("as-at-date"));

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

      public class WriteableNonSeekableStream : DelegatedStream
      {
         public WriteableNonSeekableStream(Stream master) : base(master)
         {
         }

         public override bool CanSeek => false;

         public override bool CanRead => true;

         public override long Seek(long offset, SeekOrigin origin)
         {
            throw new NotSupportedException();
         }

         public override long Position
         {
            get => base.Position;
            set => throw new NotSupportedException();
         }
      }


   }
}
