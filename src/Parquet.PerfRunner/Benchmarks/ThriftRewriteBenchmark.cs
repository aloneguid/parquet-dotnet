using DecimalTypeThriftGen = Parquet.Thrift.DecimalType;
using DecimalTypeMy = Parquet.Meta.DecimalType;
using Parquet.Meta.Proto;
using BenchmarkDotNet.Attributes;
using Thrift.Transport.Client;
using Thrift.Protocol;
using System.IO;

namespace Parquet.PerfRunner.Benchmarks {

    [ShortRunJob]
    [MeanColumn]
    [MemoryDiagnoser]
    [MarkdownExporter]
    public class ThriftRewriteBenchmark {

        //private readonly TMemoryBufferTransport _enTransport;
        //private readonly TCompactProtocol _enProto;
        //private readonly MemoryStream _myMs = new MemoryStream();
        //private readonly ThriftCompactProtocolWriter _myWriter;
        //private readonly ThriftCompactProtocolReader _myReader;
        const string FilePath = @"C:\dev\parquet-dotnet\src\Parquet.Test\data\thrift\wide.bin";

        public ThriftRewriteBenchmark() {
            //_enTransport = new TMemoryBufferTransport(System.IO.File.ReadAllBytes(FilePath), null);
            //_enProto = new TCompactProtocol(_enTransport);
            //_myWriter = new ThriftCompactProtocolWriter(_myMs);
            //_myReader = new ThriftCompactProtocolReader(new MemoryStream(System.IO.File.ReadAllBytes(FilePath)));
        }

        [Benchmark(Baseline = true)]
        public async Task Read_Standard() {
            var r = new Parquet.Thrift.FileMetaData();
            using Stream fs = System.IO.File.OpenRead(FilePath);
            var transport = new TStreamTransport(fs, null, null);
            var proto = new TCompactProtocol(transport);
            await r.ReadAsync(proto, CancellationToken.None);
        }

        [Benchmark]
        public async Task Read_My() {
            using Stream fs = System.IO.File.OpenRead(FilePath);
            var reader = new ThriftCompactProtocolReader(fs);
            Parquet.Meta.FileMetaData.Read(reader);
        }

        /*[Benchmark]
        public async Task WriteDecimalType_en() {
            var dc = new DecimalTypeThriftGen {
                Precision = 10,
                Scale = 1
            };

            await dc.WriteAsync(_enProto, CancellationToken.None);
            byte[] buffer = _enTransport.GetBuffer();
        }

        [Benchmark]
        public async Task WriteDecimalType_MyGen() {
            var dc = new DecimalTypeMy {
                Precision = 10,
                Scale = 1 };

            dc.Write(_myWriter);
            byte[] buffer = _myMs.ToArray();
        }*/
    }
}