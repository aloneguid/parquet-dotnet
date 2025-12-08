using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.PerfRunner.Benchmarks {

    /// <summary>
    /// High level numbers to track progress over time. These can be pushed to reporting database, therefore keep stable.
    /// </summary>
    public class HighLevel {

        private const int DataSize = 1000000;
        private readonly ParquetSchema _intsSchema = new ParquetSchema(new DataField<int?>("i"));
        private DataColumn? _ints;
        private MemoryStream? _intsMs;


        [GlobalSetup]
        public async Task Setup() {
            _ints = new DataColumn(_intsSchema.GetDataFields()[0],
                Enumerable.Range(0, DataSize).Select(i => i % 4 == 0 ? (int?)null : i).ToArray(),
                null);
            _intsMs = await MakeFile(_intsSchema, _ints);
        }

        private async Task<MemoryStream> MakeFile(ParquetSchema schema, DataColumn c) {
            var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
                writer.CompressionMethod = CompressionMethod.None;
                // create a new row group in the file
                using(ParquetRowGroupWriter groupWriter = writer.CreateRowGroup()) {
                    await groupWriter.WriteColumnAsync(c);
                }
            }
            return ms;
        }

        private async Task Run(ParquetSchema schema, DataColumn c) {
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, new MemoryStream())) {
                writer.CompressionMethod = CompressionMethod.None;
                // create a new row group in the file
                using(ParquetRowGroupWriter groupWriter = writer.CreateRowGroup()) {
                    await groupWriter.WriteColumnAsync(c);
                }
            }
        }

        [Benchmark]
        public Task WriteNullableInts() {
            return Run(_intsSchema, _ints!);
        }

        public static void Run() {
            BenchmarkRunner.Run<HighLevel>();
        }
    }
}
