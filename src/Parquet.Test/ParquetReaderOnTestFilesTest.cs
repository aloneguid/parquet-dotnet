using System;
using System.IO;
using System.Reflection;
using Xunit;

namespace Parquet.Test
{
   using File = System.IO.File;

   /// <summary>
   /// Tests a set of predefined test files that they read back correct
   /// </summary>
   public class ParquetReaderOnTestFilesTest
   {
      /// <summary>
      /// +---+--------+-----------+------------+-------+----------+---------+----------+-------------------------+----------+---------------------+
      /// |id |bool_col|tinyint_col|smallint_col|int_col|bigint_col|float_col|double_col|date_string_col          |string_col|timestamp_col        |
      /// +---+--------+-----------+------------+-------+----------+---------+----------+-------------------------+----------+---------------------+
      /// |4  |true    |0          |0           |0      |0         |0.0      |0.0       |[30 33 2F 30 31 2F 30 39]|[30]      |2009-03-01 00:00:00.0|
      /// |5  |false   |1          |1           |1      |10        |1.1      |10.1      |[30 33 2F 30 31 2F 30 39]|[31]      |2009-03-01 00:01:00.0|
      /// |6  |true    |0          |0           |0      |0         |0.0      |0.0       |[30 34 2F 30 31 2F 30 39]|[30]      |2009-04-01 00:00:00.0|
      /// |7  |false   |1          |1           |1      |10        |1.1      |10.1      |[30 34 2F 30 31 2F 30 39]|[31]      |2009-04-01 00:01:00.0|
      /// |2  |true    |0          |0           |0      |0         |0.0      |0.0       |[30 32 2F 30 31 2F 30 39]|[30]      |2009-02-01 00:00:00.0|
      /// |3  |false   |1          |1           |1      |10        |1.1      |10.1      |[30 32 2F 30 31 2F 30 39]|[31]      |2009-02-01 00:01:00.0|
      /// |0  |true    |0          |0           |0      |0         |0.0      |0.0       |[30 31 2F 30 31 2F 30 39]|[30]      |2009-01-01 00:00:00.0|
      /// |1  |false   |1          |1           |1      |10        |1.1      |10.1      |[30 31 2F 30 31 2F 30 39]|[31]      |2009-01-01 00:01:00.0|
      /// +---+--------+-----------+------------+-------+----------+---------+----------+-------------------------+----------+---------------------+
      /// </summary>
      [Fact]
      public void Alltypes_plain()
      {
         using (Stream s = File.OpenRead(GetDataFilePath("alltypes_plain.parquet")))
         {
            using (var r = new ParquetReader(s))
            {
               ParquetDataSet ds = r.Read();

               Assert.Equal(11, ds.Columns.Count);

               ParquetColumn idColumn = ds.Columns[0];
               Assert.Equal("id", idColumn.Name);
               Assert.Equal(4, idColumn.ValuesFinal[0]);
               Assert.Equal(5, idColumn.ValuesFinal[1]);
               Assert.Equal(6, idColumn.ValuesFinal[2]);
               Assert.Equal(7, idColumn.ValuesFinal[3]);
               Assert.Equal(2, idColumn.ValuesFinal[4]);
               Assert.Equal(3, idColumn.ValuesFinal[5]);
               Assert.Equal(0, idColumn.ValuesFinal[6]);
               Assert.Equal(1, idColumn.ValuesFinal[7]);

               ParquetColumn boolCol = ds.Columns[1];
               Assert.Equal("bool_col", boolCol.Name);
               Assert.Equal(true, boolCol.ValuesFinal[0]);
               Assert.Equal(false, boolCol.ValuesFinal[1]);
               Assert.Equal(true, boolCol.ValuesFinal[2]);
               Assert.Equal(false, boolCol.ValuesFinal[3]);
               Assert.Equal(true, boolCol.ValuesFinal[4]);
               Assert.Equal(false, boolCol.ValuesFinal[5]);
               Assert.Equal(true, boolCol.ValuesFinal[6]);
               Assert.Equal(false, boolCol.ValuesFinal[7]);

               ParquetColumn tinyintCol = ds.Columns[2];
               Assert.Equal("tinyint_col", tinyintCol.Name);
               Assert.Equal(0, tinyintCol.ValuesFinal[0]);
               Assert.Equal(1, tinyintCol.ValuesFinal[1]);
               Assert.Equal(0, tinyintCol.ValuesFinal[2]);
               Assert.Equal(1, tinyintCol.ValuesFinal[3]);
               Assert.Equal(0, tinyintCol.ValuesFinal[4]);
               Assert.Equal(1, tinyintCol.ValuesFinal[5]);
               Assert.Equal(0, tinyintCol.ValuesFinal[6]);
               Assert.Equal(1, tinyintCol.ValuesFinal[7]);

               ParquetColumn smallintCol = ds.Columns[3];
               Assert.Equal("smallint_col", smallintCol.Name);
               Assert.Equal(0, smallintCol.ValuesFinal[0]);
               Assert.Equal(1, smallintCol.ValuesFinal[1]);
               Assert.Equal(0, smallintCol.ValuesFinal[2]);
               Assert.Equal(1, smallintCol.ValuesFinal[3]);
               Assert.Equal(0, smallintCol.ValuesFinal[4]);
               Assert.Equal(1, smallintCol.ValuesFinal[5]);
               Assert.Equal(0, smallintCol.ValuesFinal[6]);
               Assert.Equal(1, smallintCol.ValuesFinal[7]);

               ParquetColumn intCol = ds.Columns[4];
               Assert.Equal("int_col", intCol.Name);
               Assert.Equal(0, intCol.ValuesFinal[0]);
               Assert.Equal(1, intCol.ValuesFinal[1]);
               Assert.Equal(0, intCol.ValuesFinal[2]);
               Assert.Equal(1, intCol.ValuesFinal[3]);
               Assert.Equal(0, intCol.ValuesFinal[4]);
               Assert.Equal(1, intCol.ValuesFinal[5]);
               Assert.Equal(0, intCol.ValuesFinal[6]);
               Assert.Equal(1, intCol.ValuesFinal[7]);

               ParquetColumn bigintCol = ds.Columns[5];
               Assert.Equal("bigint_col", bigintCol.Name);
               Assert.Equal(0L, bigintCol.ValuesFinal[0]);
               Assert.Equal(10L, bigintCol.ValuesFinal[1]);
               Assert.Equal(0L, bigintCol.ValuesFinal[2]);
               Assert.Equal(10L, bigintCol.ValuesFinal[3]);
               Assert.Equal(0L, bigintCol.ValuesFinal[4]);
               Assert.Equal(10L, bigintCol.ValuesFinal[5]);
               Assert.Equal(0L, bigintCol.ValuesFinal[6]);
               Assert.Equal(10L, bigintCol.ValuesFinal[7]);

               ParquetColumn floatCol = ds.Columns[6];
               Assert.Equal("float_col", floatCol.Name);
               Assert.Equal((float)0.0, floatCol.ValuesFinal[0]);
               Assert.Equal((float)1.1, floatCol.ValuesFinal[1]);
               Assert.Equal((float)0, floatCol.ValuesFinal[2]);
               Assert.Equal((float)1.1, floatCol.ValuesFinal[3]);
               Assert.Equal((float)0.0, floatCol.ValuesFinal[4]);
               Assert.Equal((float)1.1, floatCol.ValuesFinal[5]);
               Assert.Equal((float)0.0, floatCol.ValuesFinal[6]);
               Assert.Equal((float)1.1, floatCol.ValuesFinal[7]);

               ParquetColumn doubleCol = ds.Columns[7];
               Assert.Equal("double_col", doubleCol.Name);
               Assert.Equal((double)0.0, doubleCol.ValuesFinal[0]);
               Assert.Equal((double)10.1, doubleCol.ValuesFinal[1]);
               Assert.Equal((double)0.0, doubleCol.ValuesFinal[2]);
               Assert.Equal((double)10.1, doubleCol.ValuesFinal[3]);
               Assert.Equal((double)0.0, doubleCol.ValuesFinal[4]);
               Assert.Equal((double)10.1, doubleCol.ValuesFinal[5]);
               Assert.Equal((double)0.0, doubleCol.ValuesFinal[6]);
               Assert.Equal((double)10.1, doubleCol.ValuesFinal[7]);

               ParquetColumn dateStringCol = ds.Columns[8];
               Assert.Equal("date_string_col", dateStringCol.Name);
               Assert.Equal("03/01/09", dateStringCol.ValuesFinal[0]);
               Assert.Equal("03/01/09", dateStringCol.ValuesFinal[1]);
               Assert.Equal("04/01/09", dateStringCol.ValuesFinal[2]);
               Assert.Equal("04/01/09", dateStringCol.ValuesFinal[3]);
               Assert.Equal("02/01/09", dateStringCol.ValuesFinal[4]);
               Assert.Equal("02/01/09", dateStringCol.ValuesFinal[5]);
               Assert.Equal("01/01/09", dateStringCol.ValuesFinal[6]);
               Assert.Equal("01/01/09", dateStringCol.ValuesFinal[7]);

               ParquetColumn stringCol = ds.Columns[9];
               Assert.Equal("string_col", stringCol.Name);
               Assert.Equal("0", stringCol.ValuesFinal[0]);
               Assert.Equal("1", stringCol.ValuesFinal[1]);
               Assert.Equal("0", stringCol.ValuesFinal[2]);
               Assert.Equal("1", stringCol.ValuesFinal[3]);
               Assert.Equal("0", stringCol.ValuesFinal[4]);
               Assert.Equal("1", stringCol.ValuesFinal[5]);
               Assert.Equal("0", stringCol.ValuesFinal[6]);
               Assert.Equal("1", stringCol.ValuesFinal[7]);


            }
         }

      }

      /// <summary>
      /// +---+--------+-----------+------------+-------+----------+---------+----------+-------------------------+----------+---------------------+
      /// |id |bool_col|tinyint_col|smallint_col|int_col|bigint_col|float_col|double_col|date_string_col          |string_col|timestamp_col        |
      /// +---+--------+-----------+------------+-------+----------+---------+----------+-------------------------+----------+---------------------+
      /// |0  |true    |0          |0           |0      |0         |0.0      |0.0       |[30 31 2F 30 31 2F 30 39]|[30]      |2009-01-01 00:00:00.0|
      /// |1  |false   |1          |1           |1      |10        |1.1      |10.1      |[30 31 2F 30 31 2F 30 39]|[31]      |2009-01-01 00:01:00.0|
      /// +---+--------+-----------+------------+-------+----------+---------+----------+-------------------------+----------+---------------------+
      /// </summary>
      [Fact]
      public void Alltypes_dictionary()
      {
         using (Stream s = File.OpenRead(GetDataFilePath("alltypes_dictionary.parquet")))
         {
            using (var r = new ParquetReader(s))
            {
               ParquetDataSet ds = r.Read();
            }
         }
      }

      [Fact]
      public void Datetypes_all()
      {
         using (Stream s = File.OpenRead(GetDataFilePath("dates.parquet")))
         {
            using (var r = new ParquetReader(s))
            {
               ParquetDataSet ds = r.Read();
            }
         }
      }

      //[Fact]
      public void Postcodes()
      {
         using (Stream s = File.OpenRead("c:\\tmp\\postcodes.plain.parquet"))
         {
            using (var r = new ParquetReader(s))
            {
               ParquetDataSet ds = r.Read();
            }
         }
      }

      private string GetDataFilePath(string name)
      {
         string thisPath = Assembly.Load(new AssemblyName("Parquet.Test")).Location;
         return Path.Combine(Path.GetDirectoryName(thisPath), "data", name);
      }
   }
}