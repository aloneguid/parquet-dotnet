using System;
using System.Collections.Generic;
using System.Numerics;
using System.Text;
using Xunit;

namespace Parquet.Test.Reader
{
   public class TestDataTest : ParquetCsvComparison
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
      public void Alltypes_plain_no_compression()
      {
         CompareFiles("alltypes_plain",
            typeof(int?),
            typeof(bool?),
            typeof(int?),
            typeof(int?),
            typeof(int?),
            typeof(long?),
            typeof(float?),
            typeof(double?),
            typeof(string),
            typeof(string),
            typeof(DateTime?));
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
      public void Alltypes_dictionary_no_compression()
      {
         CompareFiles("alltypes_dictionary",
            typeof(int?),
            typeof(bool?),
            typeof(int?),
            typeof(int?),
            typeof(int?),
            typeof(long?),
            typeof(float?),
            typeof(double?),
            typeof(string),
            typeof(string),
            typeof(DateTime?));
      }
   }
}
