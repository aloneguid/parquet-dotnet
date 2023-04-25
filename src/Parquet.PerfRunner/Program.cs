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
        case "classes":
            BenchmarkRunner.Run<Classes>();
            break;
    }
} else {
    //new VsParquetSharp().Main();
    //await new DataTypes().NullableInts();
    //var c = new Classes();
    //c.SetUp();
    //c.Serialise();
    //await ParquetReader.ReadTableFromFileAsync("C:\\Users\\alone\\Downloads\\wide_parquet\\wide_parquet.parquet");
}
