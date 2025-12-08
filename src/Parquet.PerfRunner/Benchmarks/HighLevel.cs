using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using Parquet.Data;
using Parquet.Schema;
using static Parquet.PerfRunner.Benchmarks.VersionedBenchmark;

namespace Parquet.PerfRunner.Benchmarks {

    /// <summary>
    /// High level numbers to track progress over time. These can be pushed to reporting database, therefore keep stable.
    /// </summary>
    [Config(typeof(Config))]
    [MemoryDiagnoser]
    [JsonExporterAttribute.Full]
    public class HighLevel {

        private const int DataSize = 10000000;
        private readonly ParquetSchema _intsSchema = new ParquetSchema(new DataField<int?>("i"));
        private DataColumn? _ints;
        private MemoryStream? _intsMs;
        private const string ComparedVersion = "5.4.0";


        private class Config : ManualConfig {
            public Config() {

                // https://benchmarkdotnet.org/articles/samples/IntroNuGet.html

                AddJob(Job.MediumRun
                    .WithId("local"));
                AddJob(Job.MediumRun
                    .WithId(ComparedVersion)
                    .WithMsBuildArguments($"/p:ParquetVersion={ComparedVersion}"));
            }
        }


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
