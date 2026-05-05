using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using Parquet.PerfRunner.Taxis;
using ParquetWriterNet = Parquet.ParquetWriter;

namespace Parquet.PerfRunner.Benchmarks;

[Config(typeof(NuConfig))]
[MemoryDiagnoser]
[MarkdownExporter]
public class SelfComparisonBenchmark {
    public class NuConfig : ManualConfig {
        public NuConfig() {
            Job baseJob = Job.ShortRun.WithId("Parquet.Net (local)");
            AddJob(baseJob);
            AddJob(baseJob
                .WithId("Parquet.Net 6.0.0")
                .WithMsBuildArguments("/p:ParquetVersion=6.0.0"));
        }
    }

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
        _options = LogicalEncoding.CreateOptions(_schema.Schema);
    }

    [Benchmark(Description = "Parquet.Net source")]
    public async Task ParquetNetAsync() {
        using var output = new MemoryStream();
        await using ParquetWriterNet writer = await ParquetWriterNet.CreateAsync(_schema.Schema, output, _options);
        using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();

        foreach(TaxiColumn column in _schema.Columns) {
            await column.WriteAsync(rowGroup);
        }
    }
}
