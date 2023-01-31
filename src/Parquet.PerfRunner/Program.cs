// for performance tests only

using BenchmarkDotNet.Running;
using Parquet.PerfRunner.Benchmarks;

if(args.Length == 1) {
    switch(args[0]) {
        case "write":
            BenchmarkRunner.Run<WriteBenchmark>();
            break;
    }
} else {
    //new VsParquetSharp().Main();
    await new DataTypes().NullableInts();
    //Progression.Run();
}
