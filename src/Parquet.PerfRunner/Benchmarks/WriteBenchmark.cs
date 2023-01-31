using BenchmarkDotNet.Attributes;
using Parquet.Data;
using Parquet.Schema;
using ParquetSharp;
using ParquetSharp.IO;

namespace Parquet.PerfRunner.Benchmarks {

    [ShortRunJob]
    [MeanColumn]
    [MemoryDiagnoser]
    [MarkdownExporter]
    public class WriteBenchmark : BenchmarkBase {

        [Params(typeof(int), typeof(int?))]
        public Type DataType;

        private DataField _f;
        private DataColumn _c;

        private Column _psc;
        private Array _ar;

        [GlobalSetup]
        public async Task Setup() {
            _f = new DataField("test", DataType!);
            _ar = CreateTestData(DataType);
            _c = new DataColumn(_f, _ar);

            _psc = new Column(DataType!, "test");
        }

        [Benchmark]
        public async Task ParquetNet() {
            using var ms = new MemoryStream();
            using ParquetWriter pw = await ParquetWriter.CreateAsync(new ParquetSchema(_f), ms);
            using(ParquetRowGroupWriter rgw = pw.CreateRowGroup()) {
                await rgw.WriteColumnAsync(_c);
            }
        }

        [Benchmark]
        public async Task ParquetSharp() {
            using var ms = new MemoryStream();
            using var writer = new ManagedOutputStream(ms);
            using var fileWriter = new ParquetFileWriter(writer, new[] { _psc });
            using RowGroupWriter rowGroup = fileWriter.AppendRowGroup();

            if(DataType == typeof(int)) {
                using(LogicalColumnWriter<int> w = rowGroup.NextColumn().LogicalWriter<int>()) {
                    w.WriteBatch((int[])_ar);
                }
            } else if(DataType == typeof(int?)) {
                using(LogicalColumnWriter<int?> w = rowGroup.NextColumn().LogicalWriter<int?>()) {
                    w.WriteBatch((int?[])_ar);
                }
            } else if(DataType == typeof(string)) {
                using(LogicalColumnWriter<string> w = rowGroup.NextColumn().LogicalWriter<string>()) {
                    w.WriteBatch((string[])_ar);
                }
            } else {
                throw new InvalidOperationException($"don't know {DataType}");
            }

            fileWriter.Close();
        }
    }
}
