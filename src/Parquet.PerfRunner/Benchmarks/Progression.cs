using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using Microsoft.Diagnostics.Tracing.Parsers.Kernel;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.PerfRunner.Benchmarks {

    [Config(typeof(NuConfig))]
    //[ShortRunJob]
    [MarkdownExporter]
    [MemoryDiagnoser]
    [RPlotExporter]
    public class VersionedBenchmark {
        public class NuConfig : ManualConfig {
            public NuConfig() {
                Job baseJob = Job.ShortRun;

                //AddJob(CreatePackageJob(baseJob, "4.2.3"));
                //AddJob(CreatePackageJob(baseJob, "4.3.0"));
                //AddJob(CreatePackageJob(baseJob, "4.3.2"));
                //AddJob(CreatePackageJob(baseJob, "4.4.1"));
                AddJob(CreatePackageJob(baseJob, "4.5.0"));
                AddJob(CreatePackageJob(baseJob, "4.9.1"));
                AddJob(CreatePackageJob(baseJob, "4.12.0"));
                AddJob(CreatePackageJob(baseJob, "4.13.0"));
                AddJob(CreatePackageJob(baseJob, "4.14.0"));
                AddJob(CreatePackageJob(baseJob, "4.15.0"));
                AddJob(CreatePackageJob(baseJob, "4.16.0"));
                AddJob(CreatePackageJob(baseJob, "4.16.1"));
                AddJob(CreatePackageJob(baseJob, "4.16.2"));
                AddJob(CreatePackageJob(baseJob, "4.16.3"));
                AddJob(CreatePackageJob(baseJob, "4.16.4"));
                AddJob(CreatePackageJob(baseJob, "4.17.0"));
                AddJob(CreatePackageJob(baseJob, "4.18.0"));
                AddJob(CreatePackageJob(baseJob, "4.18.1"));
                AddJob(CreatePackageJob(baseJob, "4.19.0"));
                AddJob(CreatePackageJob(baseJob, "4.20.0"));
                AddJob(CreatePackageJob(baseJob, "4.20.1"));
                AddJob(CreatePackageJob(baseJob, "4.22.0"));
                AddJob(CreatePackageJob(baseJob, "4.22.1"));
                AddJob(CreatePackageJob(baseJob, "4.23.0"));
                AddJob(CreatePackageJob(baseJob, "4.23.1"));
                AddJob(CreatePackageJob(baseJob, "4.23.2"));
                AddJob(CreatePackageJob(baseJob, "4.23.3"));
                AddJob(CreatePackageJob(baseJob, "4.23.4"));
                AddJob(CreatePackageJob(baseJob, "4.23.5"));
                AddJob(CreatePackageJob(baseJob, "4.24.0"));
                AddJob(CreatePackageJob(baseJob, "4.25.0"));
                AddJob(CreatePackageJob(baseJob, "5.0.0"));
                AddJob(CreatePackageJob(baseJob, "5.0.1"));
                AddJob(CreatePackageJob(baseJob, "5.0.2"));
                AddJob(CreatePackageJob(baseJob, "5.1.0"));
                AddJob(CreatePackageJob(baseJob, "5.1.1"));
                AddJob(CreatePackageJob(baseJob, "5.2.0"));
                AddJob(CreatePackageJob(baseJob, "5.3.0"));
                AddJob(CreatePackageJob(baseJob, "5.4.0"));
            }

            private static Job CreatePackageJob(Job baseJob, string version) {
                return baseJob
                    .WithId($"Parquet.Net {version}")
                    .WithMsBuildArguments($"/p:ParquetNuGetVersion={version}");
            }
        }

        private const int DataSize = 1000000;
        private readonly ParquetSchema _intsSchema = new ParquetSchema(new DataField<int?>("i"));
        private DataColumn? _ints;
        private MemoryStream? _intsMs;

        #region [ Helpers ]

        private static readonly Random random = new Random();
        public static string RandomString(int length) {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        #endregion

        [Params(LogicalEncoding.Plain, LogicalEncoding.RleDictionary, LogicalEncoding.DeltaBinaryPacked)]
        public LogicalEncoding LogicalEncoding { get; set; }

        ParquetOptions _options = null!;

        [GlobalSetup]
        public async Task Setup() {
            _options = LogicalEncoding.CreateOptions();
            _ints = new DataColumn(_intsSchema.GetDataFields()[0],
                Enumerable.Range(0, DataSize).Select(i => i % 4 == 0 ? (int?)null : i).ToArray(),
                null);
            _intsMs = await MakeFile(_intsSchema, _ints);

            /*_nullableInts = new DataColumn(new DataField<int?>("c"),
                Enumerable
                    .Range(0, DataSize)
                    .Select(i => i % 4 == 0 ? (int?)null : i)
                    .ToArray());
            _nullableIntsMs = await MakeFile(_nullableInts);

            _doubles = new DataColumn(new DataField<double>("c"),
                Enumerable.Range(0, DataSize)
                .Select(i => (double)i)
                .ToArray());
            _doublesMs = await MakeFile(_doubles);

            _nullableDoubles = new DataColumn(new DataField<double?>("c"),
                Enumerable.Range(0, DataSize)
                .Select(i => i % 4 == 0 ? (double?)null : (double)i)
                .ToArray());
            _nullableDoublesMs = await MakeFile(_nullableDoubles);

            _randomStrings = new DataColumn(new DataField<string>("c"),
                Enumerable.Range(0, DataSize)
                .Select(i => RandomString(50))
                .ToArray());
            _randomStringsMs = await MakeFile(_randomStrings);

            _repeatingStrings = new DataColumn(new DataField<string>("c"),
                Enumerable.Range(0, DataSize)
                .Select(i => i < DataSize / 2 ? "first half" : "second half")
                .ToArray());
            _repeatedStringsMs = await MakeFile(_repeatingStrings);*/
        }

        private async Task<MemoryStream> MakeFile(ParquetSchema schema, DataColumn c) {
            var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, _options)) {
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

        private async Task Run(DataColumn c, MemoryStream ms) {
            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                Task<DataColumn[]> columns = reader.ReadEntireRowGroupAsync();
            }
            ms.Position = 0;
        }


        //[Benchmark]
        //public Task ReadInts() {
        //    return Run(_ints, _intsMs);
        //}

        [Benchmark]
        public Task WriteNullableInts() {
            return Run(_intsSchema, _ints!);
        }

        [Benchmark]
        public Task ReadNullableInts() {
            return Run(_ints!, _intsMs!);
        }

        //    [Benchmark]
        //    public Task WriteDoubles() {
        //        return Run(_doubles);
        //    }
        //
        //    [Benchmark]
        //    public Task ReadDoubles() {
        //        return Run(_doubles, _doublesMs);
        //    }
        //
        //    [Benchmark]
        //    public Task WriteNullableDoubles() {
        //        return Run(_nullableDoubles);
        //    }
        //
        //    [Benchmark]
        //    public Task ReadNullableDoubles() {
        //        return Run(_nullableDoubles, _nullableDoublesMs);
        //    }
        //
        //    [Benchmark]
        //    public Task WriteRandomStrings() {
        //        return Run(_randomStrings);
        //    }
        //
        //    [Benchmark]
        //    public Task ReadRandomStrings() {
        //        return Run(_randomStrings, _randomStringsMs);
        //    }
        //
        //    [Benchmark]
        //    public Task WriteRepeatingStrings() {
        //        return Run(_repeatingStrings);
        //    }
        //
        //    [Benchmark]
        //    public Task ReadRepeatingStrings() {
        //        return Run(_repeatingStrings, _repeatedStringsMs);
        //    }
    }
}
