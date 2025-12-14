using BenchmarkDotNet.Attributes;
using Parquet.Data;
using Parquet.Meta;
using Parquet.PerfRunner.Taxis;
using ParquetSharp;
using ParquetSharp.IO;
using IOFile = System.IO.File;
using ParquetWriterNet = Parquet.ParquetWriter;

namespace Parquet.PerfRunner.Benchmarks;

[MemoryDiagnoser]
[MarkdownExporter]
[ShortRunJob]
public class SelfComparisonBenchmark {
    [Params("tripdata", "tripdata-large")]
    public string Dataset { get; set; } = "tripdata";

    [Params(LogicalEncoding.Plain, LogicalEncoding.RleDictionary, LogicalEncoding.DeltaBinaryPacked)]
    public LogicalEncoding LogicalEncoding { get; set; }

    TaxiSchema _schema = null!;
    TaxiDataset _dataset;
    ParquetOptions _options = null!;
    [GlobalSetup]
    public async Task LoadDatasetAsync() {
        _dataset = await TaxiDatasetLoader.Instance.LoadAsync(Dataset);
        _schema = TaxiSchema.Full(_dataset);
        _options = LogicalEncoding.CreateOptions();
    }

    [Benchmark(Description = "Parquet.Net source")]
    public async Task ParquetNetAsync() {
        using var output = new MemoryStream();
        using ParquetWriterNet writer = await ParquetWriterNet.CreateAsync(_schema.Schema, output, _options);
        using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();

        foreach(DataColumn column in _schema.Columns) {
            await rowGroup.WriteColumnAsync(column);
        }
    }
}
