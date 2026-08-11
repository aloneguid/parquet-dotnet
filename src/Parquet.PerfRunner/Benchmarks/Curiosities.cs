using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Parquet.PerfRunner.Benchmarks;

[MediumRunJob]
[MeanColumn]
[MemoryDiagnoser]
[MarkdownExporter]
public class Curiosities {

    [Params(10, 10_000)]
    public int DataSize = 1;

    private int[] _ints;

    private static readonly Random random = new Random();

    [GlobalSetup]
    public void SetUp() {
        //_ints = Enumerable.Range(0, DataSize).ToArray();
        _ints = Enumerable.Range(0, DataSize).Select(i => random.Next(10)).ToArray();
    }


    [Benchmark]
    public int NaiveMin() {
        if(_ints.Length == 0)
            return default;

        int min = _ints[0];
        for(int i = 1; i < _ints.Length; i++) {
            if(_ints[i] < min) {
                min = _ints[i];
            }
        }

        return min;
    }

    [Benchmark]
    public int MathMin() {
        if(_ints.Length == 0)
            return default;

        int min = _ints[0];
        for(int i = 1; i < _ints.Length; i++) {
            min = Math.Min(min, _ints[i]);
        }

        return min;
    }


    [Benchmark]
    public int LinqMin() {
        return _ints.Length == 0 ? default : _ints.Min();
    }

}
