using System;
using System.IO;
using System.Reflection;
using Xunit;


namespace Parquet.Test.Reader
{
   [UseCulture("en-US")]
   // [UseCulture("da-DK")] // FAILS
   public class TestDataTest : ParquetCsvComparison
   {
      public TestDataTest()
      {
      }

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
         CompareFiles("types/alltypes", "plain", true,
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
            typeof(DateTimeOffset?));
      }

      [Fact]
      public void Alltypes_gzip_compression()
      {
         CompareFiles("types/alltypes", "gzip", true,
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
            typeof(DateTimeOffset?));
      }

      [Fact]
      public void Alltypes_snappy_compression()
      {
         CompareFiles("types/alltypes", "snappy", true,
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
            typeof(DateTimeOffset?));
      }

      public void Alltypes_plain_no_compression_byte_arrays()
      {
         CompareFiles("types/alltypes", "plain", false,
            typeof(int?),
            typeof(bool?),
            typeof(int?),
            typeof(int?),
            typeof(int?),
            typeof(long?),
            typeof(float?),
            typeof(double?),
            typeof(byte[]),
            typeof(byte[]),
            typeof(DateTimeOffset?));
      }

      [Fact]
      public void Alltypes_gzip_compression_byte_arrays()
      {
         CompareFiles("types/alltypes", "gzip", false,
            typeof(int?),
            typeof(bool?),
            typeof(int?),
            typeof(int?),
            typeof(int?),
            typeof(long?),
            typeof(float?),
            typeof(double?),
            typeof(byte[]),
            typeof(byte[]),
            typeof(DateTimeOffset?));
      }

      [Fact]
      public void Alltypes_snappy_compression_byte_arrays()
      {
         CompareFiles("types/alltypes", "snappy", false,
            typeof(int?),
            typeof(bool?),
            typeof(int?),
            typeof(int?),
            typeof(int?),
            typeof(long?),
            typeof(float?),
            typeof(double?),
            typeof(byte[]),
            typeof(byte[]),
            typeof(DateTimeOffset?));
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
         CompareFiles("types/alltypes_dictionary", "plain", true,
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
            typeof(DateTimeOffset?));
      }

      [Fact]
      public void Alltypes_dictionary_no_compression_by_spark()
      {
         CompareFiles("types/alltypes_dictionary", "plain-spark21", true,
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
            typeof(DateTimeOffset?));
      }

      [Fact]
      public void Alltypes_dictionary_gzipped()
      {
         CompareFiles("types/alltypes_dictionary", "gzip", true,
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
            typeof(DateTimeOffset?));
      }

      [Fact]
      public void Alltypes_dictionary_no_compression_byte_arrays()
      {
         CompareFiles("types/alltypes_dictionary", "plain", false,
            typeof(int?),
            typeof(bool?),
            typeof(int?),
            typeof(int?),
            typeof(int?),
            typeof(long?),
            typeof(float?),
            typeof(double?),
            typeof(byte[]),
            typeof(byte[]),
            typeof(DateTimeOffset?));
      }

      [Fact]
      public void Alltypes_dictionary_no_compression_by_spark_byte_arrays()
      {
         CompareFiles("types/alltypes_dictionary", "plain-spark21", false,
            typeof(int?),
            typeof(bool?),
            typeof(int?),
            typeof(int?),
            typeof(int?),
            typeof(long?),
            typeof(float?),
            typeof(double?),
            typeof(byte[]),
            typeof(byte[]),
            typeof(DateTimeOffset?));
      }

      [Fact]
      public void Alltypes_dictionary_gzipped_byte_arrays()
      {
         CompareFiles("types/alltypes_dictionary", "gzip", false,
            typeof(int?),
            typeof(bool?),
            typeof(int?),
            typeof(int?),
            typeof(int?),
            typeof(long?),
            typeof(float?),
            typeof(double?),
            typeof(byte[]),
            typeof(byte[]),
            typeof(DateTimeOffset?));
      }


      [Fact]
      public void Postcodes_sample_no_compression()
      {
         CompareFiles("postcodes", "plain", true,
            typeof(string),   //Postcode
            typeof(string),   //
            typeof(double?),
            typeof(double?),
            typeof(int?),     //Easting
            typeof(int?),     //Northing
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(string),   //Constituency
            typeof(DateTimeOffset?),
            typeof(DateTimeOffset?),
            typeof(string),   //Parish
            typeof(string),   //NationalPark
            typeof(int?),     //Population
            typeof(int?),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(int?),
            typeof(int?),
            typeof(string));
      }
   }
}