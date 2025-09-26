using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;


namespace Parquet.Test.Reader {
    [UseCulture("en-US")]
    // [UseCulture("da-DK")] // FAILS
    public class TestDataTest : ParquetCsvComparison {
        
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
        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task Alltypes_plain_no_compression(string dataPageVersion) {
            await CompareFilesAsync("types/alltypes", "plain", dataPageVersion, true,
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

        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task Alltypes_gzip_compression(string dataPageVersion) {
            await CompareFilesAsync("types/alltypes", "gzip", dataPageVersion, true,
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

        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task Alltypes_snappy_compression(string dataPageVersion) {
            await CompareFilesAsync("types/alltypes", "snappy", dataPageVersion, true,
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

        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task Alltypes_plain_no_compression_byte_arrays(string dataPageVersion) {
            await CompareFilesAsync("types/alltypes", "plain", dataPageVersion, false,
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
               typeof(DateTime?));
        }

        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task Alltypes_gzip_compression_byte_arrays(string dataPageVersion) {
            await CompareFilesAsync("types/alltypes", "gzip", dataPageVersion, false,
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
               typeof(DateTime?));
        }

        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task Alltypes_snappy_compression_byte_arrays(string dataPageVersion) {
            await CompareFilesAsync("types/alltypes", "snappy", dataPageVersion, false,
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
        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task Alltypes_dictionary_no_compression(string dataPageVersion) {
            await CompareFilesAsync("types/alltypes_dictionary", "plain", dataPageVersion, true,
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

        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task Alltypes_dictionary_no_compression_by_spark(string dataPageVersion) {
            await CompareFilesAsync("types/alltypes_dictionary", "plain-spark21", dataPageVersion, true,
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

        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task Alltypes_dictionary_gzipped(string dataPageVersion) {
            await CompareFilesAsync("types/alltypes_dictionary", "gzip", dataPageVersion, true,
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

        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task Alltypes_dictionary_no_compression_byte_arrays(string dataPageVersion) {
            await CompareFilesAsync("types/alltypes_dictionary", "plain", dataPageVersion, false,
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
               typeof(DateTime?));
        }

        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task Alltypes_dictionary_no_compression_by_spark_byte_arrays(string dataPageVersion) {
            await CompareFilesAsync("types/alltypes_dictionary", "plain-spark21", dataPageVersion, false,
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
               typeof(DateTime?));
        }

        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task Alltypes_dictionary_gzipped_byte_arrays(string dataPageVersion) {
            await CompareFilesAsync("types/alltypes_dictionary", "gzip", dataPageVersion, false,
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
               typeof(DateTime?));
        }


        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task Postcodes_sample_no_compression(string dataPageVersion) {
            await CompareFilesAsync("postcodes", "plain", dataPageVersion, true,
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
               typeof(DateTime?),
               typeof(DateTime?),
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

        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task ReadSampleFile(string dataPageVersion) {
            await CompareFilesAsync("table", "", dataPageVersion, false,
                typeof(double?), typeof(string), typeof(bool?), typeof(DateTime?), typeof(DateTime?), typeof(string));
        }

        [Fact]
        public async Task DeltaBinaryPacked() {

            Type[] types = Enumerable.Repeat(typeof(long?), 65).Concat(Enumerable.Repeat(typeof(int?), 1)).ToArray();

            await CompareFilesAsync("delta_binary_packed", "", "", false, types);
        }

        [Fact]
        public async Task DeltaLengthByteArray() {

            Type[] types = new[] { typeof(string) };

            await CompareFilesAsync("delta_length_byte_array", "", "", false, types);
        }

        [Fact]
        public async Task DeltaByteArray() {

            Type[] types = Enumerable.Repeat(typeof(string), 9).ToArray();

            await CompareFilesAsync("delta_byte_array", "", "", false, types);
        }
        
        [Theory]
        [InlineData("")]
        [InlineData("v2")]
        public async Task DecimalTypes(string dataPageVersion) {
            await CompareFilesAsync("special/decimallegacy", "", dataPageVersion, true, 
                typeof(int?),
                typeof(decimal?),
                typeof(decimal?),
                typeof(decimal?));
        }
        
        [Theory]
        [InlineData("")]
        public async Task DecimalPrecision(string dataPageVersion) {
            await CompareFilesAsync("types/decimal_precision", "", dataPageVersion, true, 
                typeof(decimal?),
                typeof(decimal?));
        }
    }
}