using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.PerfRunner.Benchmarks; 

/// <summary>
/// High level numbers to track progress over time. These can be pushed to reporting database, therefore keep stable.
/// </summary>
[Config(typeof(Config))]
[MemoryDiagnoser]
public class HighLevel {

    private const int DataSize = 10000000;
    private readonly ParquetSchema _intsSchema = new ParquetSchema(new DataField<int?>("i"));
    private DataColumn? _ints;
    private MemoryStream? _intsMs;
    private static readonly string[] TargetVersions = [
        "",
        "5.4.0"];

    [Params(CompressionMethod.None, CompressionMethod.Snappy)]
    public CompressionMethod CM { get; set; }

    private class Config : ManualConfig {
        public Config() {

            // https://benchmarkdotnet.org/articles/samples/IntroNuGet.html

            foreach(string v in TargetVersions) {
                AddJob(Job.ShortRun
                    .WithId(string.IsNullOrEmpty(v) ? "local" : v)
                    .WithMsBuildArguments($"/p:ParquetVersion={v}"));
            }
        }
    }


    [GlobalSetup]
    public async Task Setup() {
        _ints = new DataColumn(_intsSchema.GetDataFields()[0],
            Enumerable.Range(0, DataSize).Select(i => i % 4 == 0 ? (int?)null : i).ToArray(),
            null);
        _intsMs = await MakeFile(_intsSchema, _ints);

        Console.WriteLine($"parquet version: {Parquet.Globals.Version}");
    }

    private async Task<MemoryStream> MakeFile(ParquetSchema schema, DataColumn c) {
        var ms = new MemoryStream();
        using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
            writer.CompressionMethod = CM;
            // create a new row group in the file
            using(ParquetRowGroupWriter groupWriter = writer.CreateRowGroup()) {
                await groupWriter.WriteColumnAsync(c);
            }
        }
        return ms;
    }

    private async Task<MemoryStream> Run(ParquetSchema schema, DataColumn c) {
        var r = new MemoryStream();
        using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, r)) {
            writer.CompressionMethod = CM;
            // create a new row group in the file
            using(ParquetRowGroupWriter groupWriter = writer.CreateRowGroup()) {
                await groupWriter.WriteColumnAsync(c);
            }
        }
        return r;
    }

    private async Task<MemoryStream> Run(DataColumn c, MemoryStream ms) {
        ms.Position = 0;
        using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
            await reader.ReadEntireRowGroupAsync();
        }
        ms.Position = 0;
        return ms;
    }

    [Benchmark]
    public Task<MemoryStream> WriteNullableInts() {
        return Run(_intsSchema, _ints!);
    }

    [Benchmark]
    public Task<MemoryStream> ReadNullableInts() {
        return Run(_ints!, _intsMs!);
    }

    public static void Run() {
        BenchmarkRunner.Run<HighLevel>();
    }
}
