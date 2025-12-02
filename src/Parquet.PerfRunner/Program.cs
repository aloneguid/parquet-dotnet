// for performance tests only

using BenchmarkDotNet.Running;
using Parquet;
using Parquet.PerfRunner.Benchmarks;

if(args.Length == 1) {
    switch(args[0]) {
        case "write":
            BenchmarkRunner.Run<WriteBenchmark>();
            break;
        case "progression":
            VersionedBenchmark.Run();
            break;
        case "taxi":
            BenchmarkRunner.Run<TaxiCsvToParquetBenchmark>();
            break;
    }
} else {
    await new DataTypes().NullableInts();
}
