using BenchmarkDotNet.Attributes;
using Parquet.Data;
using Parquet.Meta;
using Parquet.PerfRunner.Taxis;
using ParquetSharp;
using ParquetSharp.IO;
using IOFile = System.IO.File;
using ParquetSharpEncoding = ParquetSharp.Encoding;
using ParquetWriterNet = Parquet.ParquetWriter;

namespace Parquet.PerfRunner.Benchmarks;

[MemoryDiagnoser]
[MarkdownExporter]
[ShortRunJob]
public class ParquetSharpComparisonBenchmark {

    [Params("tripdata", "tripdata-large")]
    public string Dataset { get; set; } = "tripdata";

    [Params("small", "full")]
    public string Schema { get; set; } = "small";

    [Params(LogicalEncoding.Plain, LogicalEncoding.RleDictionary, LogicalEncoding.DeltaBinaryPacked)]
    public LogicalEncoding LogicalEncoding { get; set; }


    TaxiSchema _parquetNetSchema = null!;
    ParquetSharpTaxiSchema _parquetSharpSchema = null!;
    ParquetOptions _parquetNetOptions = null!;
    WriterProperties _parquetSharpOptions = null!;
    readonly MemoryStream _memoryStream = new(2_000_000_000); // 2GB capacity so it wont cause allocations
    TaxiDataset _dataset;

    string GetFileName(string libName) =>
        $"taxi-{Dataset}-schema_{Schema}-{libName}-{LogicalEncoding.ToString().ToLowerInvariant()}.parquet";

    [GlobalSetup]
    public async Task LoadDatasetAsync() {
        _dataset = await TaxiDatasetLoader.Instance.LoadAsync(Dataset);
        if(Schema == "small") {
            _parquetNetSchema = TaxiSchema.Small(_dataset);
            _parquetSharpSchema = ParquetSharpTaxiSchema.Small();
        } else {
            _parquetNetSchema = TaxiSchema.Full(_dataset);
            _parquetSharpSchema = ParquetSharpTaxiSchema.Full();
        }
        _parquetNetOptions = LogicalEncoding.CreateOptions();
        _parquetSharpOptions = _parquetSharpSchema.CreateParquetSharpWriterProperties(LogicalEncoding);
    }

    [Benchmark(Description = "Parquet.Net -> MemoryStream")]
    public async Task ParquetNetAsync() {
        _memoryStream.Position = 0;
        using ParquetWriterNet writer = await ParquetWriterNet.CreateAsync(_parquetNetSchema.Schema, _memoryStream, _parquetNetOptions);
        writer.CompressionMethod = CompressionMethod.Snappy;
        using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();

        foreach(DataColumn column in _parquetNetSchema.Columns) {
            await rowGroup.WriteColumnAsync(column);
        }
    }

    [Benchmark(Description = "Parquet.Net -> Disk")]
    public async Task ParquetNetToDiskAsync() {
        string path = Path.Combine(Path.GetTempPath(), GetFileName("parquetnet"));
        if(Path.Exists(path))
            IOFile.Delete(path);

        try {
            await using FileStream output = IOFile.Create(path);
            using ParquetWriterNet writer = await ParquetWriterNet.CreateAsync(_parquetNetSchema.Schema, output, _parquetNetOptions);
            writer.CompressionMethod = CompressionMethod.Snappy;
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();

            foreach(DataColumn column in _parquetNetSchema.Columns) {
                await rowGroup.WriteColumnAsync(column);
            }
        } finally {
            //IOFile.Delete(path);
        }
    }

    [Benchmark(Description = "ParquetSharp -> MemoryStream")]
    public void ParquetSharp() {
        _memoryStream.Position = 0;
        using var managedOutput = new ManagedOutputStream(_memoryStream, leaveOpen: true);
        using var writer = new ParquetFileWriter(managedOutput, _parquetSharpSchema.Columns, _parquetSharpOptions);
        using RowGroupWriter rowGroup = writer.AppendRowGroup();

        _parquetNetSchema.WriteParquetSharp(rowGroup, _dataset);
        writer.Close();
    }

    [Benchmark(Description = "ParquetSharp -> Disk")]
    public void ParquetSharpToDisk() {
        string path = Path.Combine(Path.GetTempPath(), GetFileName("parquetsharp"));
        if(Path.Exists(path))
            IOFile.Delete(path);
        try {
            using FileStream output = IOFile.Create(path);
            using var managedOutput = new ManagedOutputStream(output);
            using var writer = new ParquetFileWriter(managedOutput, _parquetSharpSchema.Columns, _parquetSharpOptions);
            using RowGroupWriter rowGroup = writer.AppendRowGroup();

            _parquetNetSchema.WriteParquetSharp(rowGroup, _dataset);

            writer.Close();
        } finally {
            //IOFile.Delete(path);
        }
    }
}
