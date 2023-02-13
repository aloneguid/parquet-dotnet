
BenchmarkDotNet=v0.13.4, OS=Windows 11 (10.0.22621.1194)
Intel Core i7-1065G7 CPU 1.30GHz, 1 CPU, 8 logical and 4 physical cores
.NET SDK=7.0.102
  [Host]   : .NET 7.0.2 (7.0.222.60605), X64 RyuJIT AVX2
  ShortRun : .NET 7.0.2 (7.0.222.60605), X64 RyuJIT AVX2

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

       Method | DataType |       Mean |       Error |     StdDev |      Gen0 |     Gen1 |     Gen2 |    Allocated |
------------- |--------- |-----------:|------------:|-----------:|----------:|---------:|---------:|-------------:|
   **ParquetNet** |    **Int32** |   **4.268 ms** |   **8.5292 ms** |  **0.4675 ms** |  **390.6250** | **390.6250** | **390.6250** |  **15632.65 KB** |
 ParquetSharp |    Int32 |  29.645 ms |   6.5701 ms |  0.3601 ms |  625.0000 | 625.0000 | 625.0000 |  15383.08 KB |
   **ParquetNet** |   **Int32?** |   **2.888 ms** |   **0.3471 ms** |  **0.0190 ms** |   **50.7813** |        **-** |        **-** |    **201.47 KB** |
 ParquetSharp |   Int32? |   4.941 ms |   0.6049 ms |  0.0332 ms |   31.2500 |        - |        - |     147.1 KB |
   **ParquetNet** |   **String** | **290.043 ms** | **224.9942 ms** | **12.3327 ms** | **7500.0000** |        **-** |        **-** | **189399.37 KB** |
 ParquetSharp |   String | 231.025 ms |  45.4520 ms |  2.4914 ms | 8333.3333 | 666.6667 | 666.6667 |  94367.57 KB |
