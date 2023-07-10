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
    [RPlotExporter]
    public class WriteBenchmark : BenchmarkBase {

        [Params(typeof(int), typeof(int?), typeof(double), typeof(double?))]
        //[Params(typeof(string))]
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        public Type DataType;
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

        private ParquetSchema? _schema;
        private DataColumn? _c;

        private Column? _psc;
        private Array? _ar;

        [GlobalSetup]
        public Task SetupAsync() {
            _schema = new ParquetSchema(new DataField("test", DataType!));
            _ar = CreateTestData(DataType);
            _c = new DataColumn(_schema.DataFields[0], _ar);

            _psc = new Column(DataType!, "test");

            return Task.CompletedTask;
        }

        [Benchmark]
        public async Task ParquetNet() {
            using var ms = new MemoryStream();
            using ParquetWriter pw = await ParquetWriter.CreateAsync(_schema!, ms);
            using(ParquetRowGroupWriter rgw = pw.CreateRowGroup()) {
                await rgw.WriteColumnAsync(_c!);
            }
        }

        [Benchmark]
        public void ParquetSharp() {
            using var ms = new MemoryStream();
            using var writer = new ManagedOutputStream(ms);
            using var fileWriter = new ParquetFileWriter(writer, new[] { _psc! });
            using RowGroupWriter rowGroup = fileWriter.AppendRowGroup();

            if(DataType == typeof(int)) {
                using(LogicalColumnWriter<int> w = rowGroup.NextColumn().LogicalWriter<int>()) {
                    w.WriteBatch((int[])_ar!);
                }
            } else if(DataType == typeof(int?)) {
                using(LogicalColumnWriter<int?> w = rowGroup.NextColumn().LogicalWriter<int?>()) {
                    w.WriteBatch((int?[])_ar!);
                }
            } else if(DataType == typeof(double)) {
                using(LogicalColumnWriter<double> w = rowGroup.NextColumn().LogicalWriter<double>()) {
                    w.WriteBatch((double[])_ar!);
                }
            } else if(DataType == typeof(double?)) {
                using(LogicalColumnWriter<double?> w = rowGroup.NextColumn().LogicalWriter<double?>()) {
                    w.WriteBatch((double?[])_ar!);
                }
            } else if(DataType == typeof(string)) {
                using(LogicalColumnWriter<string> w = rowGroup.NextColumn().LogicalWriter<string>()) {
                    w.WriteBatch((string[])_ar!);
                }
            } else {
                throw new InvalidOperationException($"don't know {DataType}");
            }

            fileWriter.Close();
        }
    }
}
