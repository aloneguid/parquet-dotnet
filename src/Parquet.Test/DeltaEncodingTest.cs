using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test {
    public class DeltaEncodingTest: TestBase{
        [Fact]
        public async Task DeltaEncodingInt32Test() {

            int[] data = new[] { 7, 5, 3, 1, -2, 3, 4, -5, };


            var dataField = new DataField<int>("column1");
            var parquetSchema = new ParquetSchema(dataField);

            using var stream = new MemoryStream();
            ParquetOptions options = new ParquetOptions() {
                ColumnEncoding = new Dictionary<string, Meta.Encoding>() {
                    { "column1", Meta.Encoding.DELTA_BINARY_PACKED }
                }
            };
            using(ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(parquetSchema, stream, formatOptions: options)) {
                using ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup();
                await groupWriter.WriteColumnAsync(new DataColumn(dataField, data));
            }

            using ParquetReader parquetReader = await ParquetReader.CreateAsync(stream);
            using ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(0);
            Data.DataColumn dataColumn = await groupReader.ReadColumnAsync(dataField);

            Assert.Equal(data, dataColumn.Data);
        }

        [Fact]
        public async Task DeltaEncodingInt64Test() {

            long[] data = new[] { 7L, 5L, 3L, 1L, -2L, 3L, 4L, -5L, };


            var dataField = new DataField<long>("column1");
            var parquetSchema = new ParquetSchema(dataField);

            using var stream = new MemoryStream();
            ParquetOptions options = new ParquetOptions() {
                ColumnEncoding = new Dictionary<string, Meta.Encoding>() {
                    { "column1", Meta.Encoding.DELTA_BINARY_PACKED }
                }
            };
            using(ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(parquetSchema, stream, formatOptions: options)) {
                using ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup();
                await groupWriter.WriteColumnAsync(new DataColumn(dataField, data));
            }

            using ParquetReader parquetReader = await ParquetReader.CreateAsync(stream);
            using ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(0);
            Data.DataColumn dataColumn = await groupReader.ReadColumnAsync(dataField);

            Assert.Equal(data, dataColumn.Data);
        }

        [Fact]
        public async Task Should_throw_NotSupportedException() {
            double[] data = new double[]
            {
            1.5,
            };

            var dataField = new DataField<double>("column1");
            var parquetSchema = new ParquetSchema(dataField);

            using var stream = new MemoryStream();
            ParquetOptions options = new ParquetOptions() {
                ColumnEncoding = new Dictionary<string, Meta.Encoding>() {
                    { "column1", Meta.Encoding.DELTA_BINARY_PACKED }
                }
            };
            using(ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(parquetSchema, stream, formatOptions: options)) {
                using ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup();
                await Assert.ThrowsAsync<NotSupportedException>(() => groupWriter.WriteColumnAsync(new DataColumn(dataField, data)));
            }
        }
    }
}
