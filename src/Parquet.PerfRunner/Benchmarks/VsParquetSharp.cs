using System.Data.Common;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Parquet.Data;
using Parquet.Schema;
using ParquetSharp;

namespace Parquet.PerfRunner.Benchmarks {
    class VsParquetSharp {
        public void Main() {
            BenchmarkRunner.Run<ParquetBenches>();
        }
    }

    [ShortRunJob]
    [MarkdownExporter]
    [MemoryDiagnoser]
    public class ParquetBenches {
        public string ParquetNetFilename;
        public string ParquetSharpFilename;

        //[Params("int", "str", "float")]
        [Params("int?")]
        public string DataType;

        //[Params(10, 100, 1000, 1000000)]
        [Params(100000)]
        public int DataSize;

        [Params("write", "read")]
        public string Mode;


        private static Random random = new Random();
        public static string RandomString(int length) {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        private DataColumn _pqnc;
        private ParquetSchema _pqns;
        private MemoryStream _pnqMs;

        private Column[] _pqss;
        private object _pqsd;
        private Action<RowGroupWriter> _pqsWriteAction;
        private Action<RowGroupReader, int> _pqsReadAction;

        [GlobalSetup]
        public async Task Setup() {
            switch(DataType) {
                case "int":
                    _pqnc = new DataColumn(new DataField<int>("c"), Enumerable.Range(0, DataSize).ToArray());
                    _pqss = new Column[] { new Column<int>("c") };
                    _pqsd = (int[])_pqnc.Data;
                    _pqsWriteAction = w => {
                        using(LogicalColumnWriter<int> colWriter = w.NextColumn().LogicalWriter<int>()) {
                            colWriter.WriteBatch((int[])_pqsd);
                        }
                    };
                    _pqsReadAction = (r, n) => {
                        int[] data = r.Column(0).LogicalReader<int>().ReadAll(n);
                    };
                    break;
                case "int?":
                    _pqnc = new DataColumn(new DataField<int?>("c"),
                        Enumerable
                            .Range(0, DataSize)
                            .Select(i => i % 4 == 0 ? (int?)null : i)
                            .ToArray());
                    _pqss = new Column[] { new Column<int?>("c") };
                    _pqsd = (int?[])_pqnc.Data;
                    _pqsWriteAction = w => {
                        using(LogicalColumnWriter<int?> colWriter = w.NextColumn().LogicalWriter<int?>()) {
                            colWriter.WriteBatch((int?[])_pqsd);
                        }
                    };
                    _pqsReadAction = (r, n) => {
                        int?[] data = r.Column(0).LogicalReader<int?>().ReadAll(n);
                    };
                    break;
                case "str":
                    _pqnc = new DataColumn(new DataField<string>("c"), Enumerable.Range(0, DataSize).Select(i => RandomString(100)).ToArray());
                    _pqss = new Column[] { new Column<string>("c") };
                    _pqsd = (string[])_pqnc.Data;
                    _pqsWriteAction = w => {
                        using(LogicalColumnWriter<string> colWriter = w.NextColumn().LogicalWriter<string>()) {
                            colWriter.WriteBatch((string[])_pqsd);
                        }
                    };
                    _pqsReadAction = (r, n) => {
                        string[] data = r.Column(0).LogicalReader<string>().ReadAll(n);
                    };
                    break;
                case "float":
                    _pqnc = new DataColumn(
                        new DataField<float>("f"), Enumerable.Range(0, DataSize).Select(i => (float)i).ToArray());
                    _pqss = new Column[] { new Column<float>("f") };
                    _pqsd = (float[])_pqnc.Data;
                    _pqsWriteAction = w => {
                        using(LogicalColumnWriter<float> colWriter = w.NextColumn().LogicalWriter<float>()) {
                            colWriter.WriteBatch((float[])_pqsd);
                        }
                    };
                    _pqsReadAction = (r, n) => {
                        float[] data = r.Column(0).LogicalReader<float>().ReadAll(n);
                    };

                    break;
                case "date":
                    _pqnc = new DataColumn(
                        new DataField<DateTime>("dto"),
                        Enumerable.Range(0, DataSize).Select(i => DateTime.UtcNow.AddSeconds(i)).ToArray());
                    _pqss = new Column[] { new Column<DateTime>("dto") };
                    _pqsd = (DateTime[])_pqnc.Data;
                    _pqsWriteAction = w => {
                        using(LogicalColumnWriter<DateTime> colWriter = w.NextColumn().LogicalWriter<DateTime>()) {
                            colWriter.WriteBatch((DateTime[])_pqsd);
                        }
                    };
                    _pqsReadAction = (r, n) => {
                        DateTime[] data = r.Column(0).LogicalReader<DateTime>().ReadAll(n);
                    };

                    break;


                default:
                    throw new NotImplementedException();
            }

            _pqns = new ParquetSchema(_pqnc.Field);
            _pnqMs = new MemoryStream(1000);

            string fileDataType = DataType.Replace("?", "_nullable");
            ParquetNetFilename = $"c:\\tmp\\parq_net_benchmark_{Mode}_{DataSize}_{fileDataType}.parquet";
            ParquetSharpFilename = $"c:\\tmp\\parq_sharp_benchmark_{Mode}_{DataSize}_{fileDataType}.parquet";

            if(Mode == "read") {
                using(Stream fileStream = System.IO.File.Create(ParquetNetFilename)) {
                    using(ParquetWriter writer = await ParquetWriter.CreateAsync(_pqns, fileStream)) {
                        writer.CompressionMethod = CompressionMethod.None;
                        // create a new row group in the file
                        using(ParquetRowGroupWriter groupWriter = writer.CreateRowGroup()) {
                            await groupWriter.WriteColumnAsync(_pqnc);
                        }
                    }
                }
            }
        }

        [Benchmark]
        public async Task ParquetNet() {
            if(Mode == "write") {
                using(Stream fileStream = System.IO.File.Create(ParquetNetFilename)) {
                    using(ParquetWriter writer = await ParquetWriter.CreateAsync(_pqns, fileStream)) {
                        writer.CompressionMethod = CompressionMethod.None;
                        // create a new row group in the file
                        using(ParquetRowGroupWriter groupWriter = writer.CreateRowGroup()) {
                            await groupWriter.WriteColumnAsync(_pqnc);
                        }
                    }
                }
            }
            else if(Mode == "read") {
                using(ParquetReader reader = await ParquetReader.CreateAsync(ParquetNetFilename)) {
                    await reader.ReadEntireRowGroupAsync();
                }
            }
        }

        [Benchmark]
        public async Task ParquetSharp() {
            //https://github.com/G-Research/ParquetSharp#low-level-api

            if(Mode == "write") {
                using(var writer = new ParquetFileWriter(ParquetSharpFilename, _pqss, Compression.Uncompressed)) {
                    using(RowGroupWriter rowGroup = writer.AppendRowGroup()) {
                        _pqsWriteAction(rowGroup);
                    }
                }
            }
            else if(Mode == "read") {
                using(var reader = new ParquetFileReader(ParquetNetFilename)) {
                    using(RowGroupReader g = reader.RowGroup(0)) {
                        int n = checked((int)g.MetaData.NumRows);
                        _pqsReadAction(g, n);
                    }
                }
            }

        }
    }
}
