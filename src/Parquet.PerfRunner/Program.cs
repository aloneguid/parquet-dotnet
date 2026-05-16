// for performance tests only

using BenchmarkDotNet.Running;
using Parquet.PerfRunner.Benchmarks;

if(args.Length == 1) {
    switch(args[0]) {
#if !PARQUET_PACKAGE
        case "highLevel":
            HighLevel.Run();
            break;
        case "write":
            BenchmarkRunner.Run<WriteBenchmark>();
            break;
        case "encoding":
            BenchmarkRunner.Run<EncodingBenchmarks>();
            break;
        case "curiosities":
            BenchmarkRunner.Run<Curiosities>();
            break;
        case "twosamples":
            break;
        case "compression":
            BenchmarkRunner.Run<CompressionBenchmarks>();
            break;
#endif
        case "sharp-compare-taxi":
            BenchmarkRunner.Run<ParquetSharpComparisonBenchmark>();
            break;
        case "self-compare-taxi":
            BenchmarkRunner.Run<SelfComparisonBenchmark>();
            break;
    }
} else {
#if !PARQUET_PACKAGE
    await new DataTypes().RandomStrings();
#endif
    //await SampleGenerator.GenerateFiles();
}
