using BenchmarkDotNet.Attributes;
using Parquet.Schema;
using ParquetSharp;
using ParquetSharp.IO;

namespace Parquet.PerfRunner.Benchmarks;

[ShortRunJob]
[MeanColumn]
[MemoryDiagnoser]
[MarkdownExporter]
//[RPlotExporter]
public class WriteBenchmark : BenchmarkBase {

    //[Params(typeof(int), typeof(int?), typeof(double), typeof(double?))]
    [Params(typeof(string), typeof(ReadOnlyMemory<char>))]
    public Type DataType = typeof(int);

    private ParquetSchema? _schema;
    //private DataColumn? _c;

    private Column? _psc;
    private Array? _ar;

    [GlobalSetup]
    public Task SetupAsync() {
        _schema = new ParquetSchema(new DataField("test", DataType!));
        _ar = CreateTestData(DataType);
        //_c = new DataColumn(_schema.DataFields[0], _ar);

        _psc = new Column(DataType!, "test");

        return Task.CompletedTask;
    }

    [Benchmark]
    public async Task ParquetNet() {
        using var ms = new MemoryStream();
        await using ParquetWriter pw = await ParquetWriter.CreateAsync(_schema!, ms, new ParquetOptions { CompressionMethod = CompressionMethod.None });
        using ParquetRowGroupWriter rgw = pw.CreateRowGroup();

        if(DataType == typeof(int)) {
            int[] data = (int[])_ar!;
            await rgw.WriteAsync<int>(_schema!.DataFields[0], data);
        } else if(DataType == typeof(int?)) {
            int?[] data = (int?[])_ar!;
            await rgw.WriteAsync<int>(_schema!.DataFields[0], data);
        } else if(DataType == typeof(double)) {
            double[] data = (double[])_ar!;
            await rgw.WriteAsync<double>(_schema!.DataFields[0], data);
        } else if(DataType == typeof(double?)) {
            double?[] data = (double?[])_ar!;
            await rgw.WriteAsync<double>(_schema!.DataFields[0], data);
        } else if(DataType == typeof(string)) {
            string[] data = (string[])_ar!;
            await rgw.WriteAsync(_schema!.DataFields[0], data);
        } else if(DataType == typeof(ReadOnlyMemory<char>)) {
            ReadOnlyMemory<char>[] data = (ReadOnlyMemory<char>[])_ar!;
            await rgw.WriteAsync<ReadOnlyMemory<char>>(_schema!.DataFields[0], data);
        } else {
            throw new InvalidOperationException($"don't know {DataType}");
        }

        rgw.CompleteValidate();
    }

    [Benchmark]
    public void ParquetSharp() {
        using var ms = new MemoryStream();
        using var writer = new ManagedOutputStream(ms);
        using var fileWriter = new ParquetFileWriter(writer, new[] { _psc! }, Compression.Uncompressed);
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
