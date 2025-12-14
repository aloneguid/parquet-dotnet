using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using Parquet.Data;
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
                .WithId("Parquet.Net 5.4.0")
                .WithMsBuildArguments("/p:ParquetVersion=5.4.0"));
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
