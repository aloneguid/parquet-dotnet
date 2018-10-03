using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Parquet.Data;
using Parquet.Data.Rows;
using Xunit;

namespace Parquet.Test
{
   public class RowsModelTest : TestBase
   {
      #region [ Flat Tables ]

      [Fact]
      public void Flat_add_valid_row_succeeds()
      {
         var table = new Table(new Schema(new DataField<int>("id")));
         table.Add(new Row(1));
      }

      [Fact]
      public void Flat_add_invalid_type_fails()
      {
         var table = new Table(new Schema(new DataField<int>("id")));

         Assert.Throws<ArgumentException>(() => table.Add(new Row("1")));
      }

      [Fact]
      public void Flat_write_read()
      {
         var table = new Table(new Schema(new DataField<int>("id"), new DataField<string>("city")));
         var ms = new MemoryStream();

         //generate fake data
         for (int i = 0; i < 1000; i++)
         {
            table.Add(new Row(i, "record#" + i));
         }

         //write to stream
         using (var writer = new ParquetWriter(table.Schema, ms))
         {
            writer.Write(table);
         }

         //read back into table
         ms.Position = 0;
         Table table2;
         using (var reader = new ParquetReader(ms))
         {
            table2 = reader.ReadAsTable();
         }

         //validate data
         Assert.True(table.Equals(table2, true));
      }

      #endregion

      #region [ Array Tables ]

      [Fact]
      public void Array_validate_succeeds()
      {
         var table = new Table(new Schema(new DataField<IEnumerable<int>>("ids")));

         table.Add(new Row(new[] { 1, 2, 3 }));
         table.Add(new Row(new[] { 4, 5, 6 }));
      }

      [Fact]
      public void Array_validate_fails()
      {
         var table = new Table(new Schema(new DataField<IEnumerable<int>>("ids")));

         Assert.Throws<ArgumentException>(() => table.Add(new Row(1)));
      }

      [Fact]
      public void Array_write_read()
      {
         var table = new Table(
            new Schema(
               new DataField<int>("id"),
               new DataField<string[]>("categories")     //array field
               )
            );
         var ms = new MemoryStream();

         table.Add(1, new[] { "1", "2", "3" });
         table.Add(3, new[] { "3", "3", "3" });

         //write to stream
         using (var writer = new ParquetWriter(table.Schema, ms))
         {
            writer.Write(table);
         }

         //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

         //read back into table
         ms.Position = 0;
         Table table2;
         using (var reader = new ParquetReader(ms))
         {
            table2 = reader.ReadAsTable();
         }

         //validate data
         Assert.Equal(table.ToString(), table2.ToString());
      }

      #endregion

      #region [ Maps ]

      [Fact]
      public void Map_validate_succeeds()
      {
         var table = new Table(new Schema(
            new MapField("map", new DataField<string>("key"), new DataField<string>("value"))
            ));

         table.Add(Row.SingleCell(
            new List<Row>
            {
               new Row("one", "v1"),
               new Row("two", "v2")
            }));
      }

      [Fact]
      public void Map_validate_fails()
      {
         var table = new Table(new Schema(
            new MapField("map", new DataField<string>("key"), new DataField<string>("value"))
            ));

         Assert.Throws<ArgumentException>(() => table.Add(new Row(1)));
      }

      [Fact]
      public void Map_read_from_Apache_Spark()
      {
         Table t;
         using (Stream stream = OpenTestFile("map_simple.parquet"))
         {
            using (var reader = new ParquetReader(stream))
            {
               t = reader.ReadAsTable();
            }
         }

         Assert.Equal("{1;[{1;one};{2;two};{3;three}]}", t[0].ToString());
      }

      [Fact]
      public void Map_write_read()
      {
         var table = new Table(
            new Schema(
               new DataField<string>("city"),
               new MapField("population",
                  new DataField<int>("areaId"),
                  new DataField<long>("count"))));
         var ms = new MemoryStream();

         table.Add("London",
            new List<Row>
            {
               new Row(234, 100L),
               new Row(235, 110L)
            });

         Table table2 = WriteRead(table);

         Assert.Equal(table.ToString(), table2.ToString());
      }

      #endregion

      #region [ Struct ]

      [Fact]
      public void Struct_read_plain_structs_from_Apache_Spark()
      {
         Table t = ReadTestFileAsTable("struct_plain.parquet");

         Assert.Equal("[{12345-6;{Ivan;Gavryliuk}};{12345-7;{Richard;Conway}}]", t.ToString());
      }

      [Fact]
      public void Struct_write_read()
      {
         var table = new Table(
            new Schema(
               new DataField<string>("isbn"),
               new StructField("author",
                  new DataField<string>("firstName"),
                  new DataField<string>("lastName"))));
         var ms = new MemoryStream();

         table.Add("12345-6", new Row("Ivan", "Gavryliuk"));
         table.Add("12345-7", new Row("Richard", "Conway"));

         Table table2 = WriteRead(table);

         Assert.Equal(table.ToString(), table2.ToString());
      }

      [Fact]
      public void Struct_with_repeated_field_writes_reads()
      {
         var t = new Table(new Schema(
            new DataField<string>("name"),
            new StructField("address",
               new DataField<string>("name"),
               new DataField<IEnumerable<string>>("lines"))));

         t.Add("Ivan", new Row("Primary", new[] { "line1", "line2" }));

         Table t2 = WriteRead(t);

         Assert.Equal("{Ivan;{Primary;[line1;line2]}}", t[0].ToString());
      }

      #endregion

      #region [ List ]

      [Fact]
      public void List_read_simple_element_from_Apache_Spark()
      {
         Table t;
         using (Stream stream = OpenTestFile("list_simple.parquet"))
         {
            using (var reader = new ParquetReader(stream))
            {
               t = reader.ReadAsTable();
            }
         }

         Assert.Equal("{[London;Derby;Paris;New York];1}", t[0].ToString());
      }

      [Fact]
      public void List_simple_element_write_read()
      {
         var table = new Table(
            new Schema(
               new DataField<int>("id"),
               new ListField("cities",
                  new DataField<string>("name"))));

         var ms = new MemoryStream();

         table.Add(1, new[] { "London", "Derby" });
         table.Add(2, new[] { "Paris", "New York" });

         //write as table
         using (var writer = new ParquetWriter(table.Schema, ms))
         {
            writer.Write(table);
         }

         //read back into table
         ms.Position = 0;
         Table table2;
         using (var reader = new ParquetReader(ms))
         {
            table2 = reader.ReadAsTable();
         }

         //validate data
         Assert.Equal(table.ToString(), table2.ToString());
      }

      [Fact]
      public void List_read_structures_from_Apache_Spark()
      {
         Table t;
         using (Stream stream = OpenTestFile("list_structs.parquet"))
         {
            using (var reader = new ParquetReader(stream))
            {
               t = reader.ReadAsTable();
            }
         }

         Assert.Single(t);
         Assert.Equal("{[{UK;London};{US;New York}];1}", t[0].ToString());
      }

      [Fact]
      public void List_read_write_structures()
      {
         Table t = new Table(
            new DataField<int>("id"),
            new ListField("structs",
               new StructField("mystruct",
                  new DataField<int>("id"),
                  new DataField<string>("name"))));

         t.Add(1, new[] { new Row(1, "Joe"), new Row(2, "Bloggs") });
         t.Add(2, new[] { new Row(3, "Star"), new Row(4, "Wars") });

         Table t2 = WriteRead(t);

         Assert.Equal(t.ToString(), t2.ToString());
      }

      //[Fact]
      public void List_of_elements_is_empty_reads_file()
      {
         Table t;
         using (Stream stream = OpenTestFile("list_empty.parquet"))
         {
            using (var reader = new ParquetReader(stream))
            {
               t = reader.ReadAsTable();
            }
         }

         Assert.Equal("{2;[]}", t[0].ToString());
      }

      //[Fact]
      public void List_of_elements_is_empty_writes_reads()
      {
         var t = new Table(
            new DataField<int>("id"),
            new ListField("strings",
               new DataField<string>("item")
            ));
         t.Add(1, new string[0]);
         Assert.Equal("{1;[]}", WriteRead(t).ToString());
      }

      //[Fact]
      public void List_of_elements_with_some_items_empty_writes_reads()
      {
         var t = new Table(
            new DataField<int>("id"),
            new ListField("strings",
               new DataField<string>("item")
            ));
         t.Add(1, new string[] { "1", "2", "3" });
         t.Add(2, new string[] { });
         t.Add(3, new string[] { "1", "2", "3" });
         t.Add(4, new string[] { });

         Table t1 = WriteRead(t);
         Assert.Equal(4, t1.Count);
         Assert.Equal("{1;[1;2;3]}", t1[0].ToString());
         Assert.Equal("{2;[]}", t1[1].ToString());
         Assert.Equal("{3;[1;2;3]}", t1[2].ToString());
         Assert.Equal("{4;[]}", t1[3].ToString());

      }

      #endregion

      #region [ Mixed ]

      #endregion

      #region [ Special Cases ]

      [Fact]
      public void Special_read_all_nulls_no_booleans()
      {
         ReadTestFileAsTable("special/all_nulls_no_booleans.parquet");
      }

      [Fact]
      public void Special_all_nulls_file()
      {
         Table t = ReadTestFileAsTable("special/all_nulls.parquet");

         Assert.Equal(1, t.Schema.Fields.Count);
         Assert.Equal("lognumber", t.Schema[0].Name);
         Assert.Single(t);
         Assert.Null(t[0][0]);
      }

      [Fact]
      public void Special_read_all_nulls_decimal_column()
      {
         ReadTestFileAsTable("special/decimalnulls.parquet");
      }

      [Fact]
      public void Special_read_all_legacy_decimals()
      {
         Table ds = ReadTestFileAsTable("special/decimallegacy.parquet");

         Row row = ds[0];
         Assert.Equal(1, (int)row[0]);
         Assert.Equal(1.2m, (decimal)row[1], 2);
         Assert.Null(row[2]);
         Assert.Equal(-1m, (decimal)row[3], 2);
      }

      [Fact]
      public void Special_read_file_with_multiple_row_groups()
      {
         var ms = new MemoryStream();

         //create multirowgroup file

         //first row group
         var t = new Table(new DataField<int>("id"));
         t.Add(1);
         t.Add(2);
         using (var writer = new ParquetWriter(t.Schema, ms))
         {
            writer.Write(t);
         }

         //second row group
         t.Clear();
         t.Add(3);
         t.Add(4);
         using (var writer = new ParquetWriter(t.Schema, ms, null, true))
         {
            writer.Write(t);
         }

         //read back as table
         t = ParquetReader.ReadTableFromStream(ms);
         Assert.Equal(4, t.Count);
      }

      #endregion

      #region [ And the Big Ultimate Fat Test!!! ]

      /// <summary>
      /// This essentially proves that we can READ complicated data structures, but not write (see other tests)
      /// </summary>
      [Fact]
      public void BigFatOne_variations_from_Apache_Spark()
      {
         Table t;
         using (Stream stream = OpenTestFile("all_var1.parquet"))
         {
            using (var reader = new ParquetReader(stream))
            {
               t = reader.ReadAsTable();
            }
         }

         Assert.Equal(2, t.Count);
         Assert.Equal("{[{Dante Road;Head Office;[9;10;11;12;13;14;15;16;17;18];SE11};{Somewhere Else;Small Office;[6;7;19;20;21;22;23];TN19}];[London;Derby];this file contains all the permunations for nested structures and arrays to test Parquet parser;1;{51.2;66.3};{{2;1}}}", t[0].ToString());
         Assert.Equal("{[{Dante Road;Head Office;[9;10;11;12;13;14;15;16;17;18];SE11};{Somewhere Else;Small Office;[6;7;19;20;21;22;23];TN19}];[London;Derby];this file contains all the permunations for nested structures and arrays to test Parquet parser;1;{51.2;66.3};{{2;1}}}", t[1].ToString());
      }

      #endregion

      #region [ JSON Conversions ]

      [Fact]
      public void JSON_reads_by_newtonsoft()
      {
         Table t = ReadTestFileAsTable("struct_plain.parquet");

         string json = t.ToString("j");

         object jsonObject = JsonConvert.DeserializeObject(json);
      }

      #endregion
   }
}