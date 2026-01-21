// for performance tests only

using BenchmarkDotNet.Running;
using Parquet;
using Parquet.PerfRunner;
using Parquet.PerfRunner.Benchmarks;

if(args.Length == 1) {
    switch(args[0]) {
        case "highLevel":
            HighLevel.Run();
            break;
        case "write":
            BenchmarkRunner.Run<WriteBenchmark>();
            break;
        case "twosamples":
            break;
    }
} else {
    await new DataTypes().NullableInts();
    //await SampleGenerator.GenerateFiles();
}
