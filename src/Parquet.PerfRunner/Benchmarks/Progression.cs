using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.PerfRunner.Benchmarks {

    [Config(typeof(NuConfig))]
    //[ShortRunJob]
    [MarkdownExporter]
    [MemoryDiagnoser]
    [RPlotExporter]
    public class VersionedBenchmark {

        public static void Run() {
            BenchmarkRunner.Run<VersionedBenchmark>();
        }

        public class NuConfig : ManualConfig {
            public NuConfig() {
                Job baseJob = Job.ShortRun;

                //AddJob(baseJob.WithNuGet("Parquet.Net", "4.2.3"));
                //AddJob(baseJob.WithNuGet("Parquet.Net", "4.3.0"));
                //AddJob(baseJob.WithNuGet("Parquet.Net", "4.3.2"));
                //AddJob(baseJob.WithNuGet("Parquet.Net", "4.4.1"));
                AddJob(baseJob.WithNuGet("Parquet.Net", "4.5.0"));
                AddJob(baseJob.WithNuGet("Parquet.Net", "4.9.1"));
                AddJob(baseJob.WithNuGet("Parquet.Net", "4.12.0"));
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

        [GlobalSetup]
        public async Task Setup() {
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
