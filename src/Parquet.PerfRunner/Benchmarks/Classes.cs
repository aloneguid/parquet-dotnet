using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Win32.SafeHandles;
using Parquet.Serialization;

namespace Parquet.PerfRunner.Benchmarks {

    class Record {
        public DateTime Timestamp { get; set; }
        public string? EventName { get; set; }
        public double MeterValue { get; set; }
    }


    [ShortRunJob]
    [MeanColumn]
    [MemoryDiagnoser]
    [MarkdownExporter]
    public class Classes {
        private List<Record>? _testData;
        private MemoryStream _ms = new MemoryStream();

        [GlobalSetup]
        public async Task SetUp() {
            _testData = Enumerable.Range(0, 1_000).Select(i => new Record {
                Timestamp = DateTime.UtcNow.AddSeconds(i),
                EventName = i % 2 == 0 ? "on" : "off",
                MeterValue = i
            }).ToList();

            await ParquetSerializer.SerializeAsync(_testData, _ms);
        }


        [Benchmark(Baseline = true)]
        public async Task Serialise_Legacy() {
            using var ms = new MemoryStream();
            await ParquetConvert.SerializeAsync(_testData, ms);
        }

        [Benchmark]
        public async Task Deserialise_Legacy() {
            _ms.Position = 0;
            await ParquetConvert.DeserializeAsync<Record>(_ms);
        }

        [Benchmark]
        public async Task Serialise() {
            using var ms = new MemoryStream();
            await ParquetSerializer.SerializeAsync(_testData, ms);
        }

        [Benchmark]
        public async Task Deserialise() {
            _ms.Position = 0;
            await ParquetSerializer.DeserializeAsync<Record>(_ms);
        }
    }
}
