using Thrift.Protocol;
using Thrift.Transport.Client;
using DecimalTypeThriftGen = Parquet.Thrift.DecimalType;
using DecimalTypeMy = Parquet.Meta.DecimalType;
using Parquet.Meta.Proto;
using BenchmarkDotNet.Attributes;

namespace Parquet.PerfRunner.Benchmarks {

    [ShortRunJob]
    [MeanColumn]
    [MemoryDiagnoser]
    [MarkdownExporter]
    public class ThriftRewriteBenchmark {

        private readonly TMemoryBufferTransport _thriftGenTransport;
        private readonly TCompactProtocol _thriftGenProto;
        private readonly MemoryStream _myMs = new MemoryStream();
        private readonly ThriftCompactProtocolWriter _myWriter;

        public ThriftRewriteBenchmark() {
            _thriftGenTransport = new TMemoryBufferTransport(null);
            _thriftGenProto = new TCompactProtocol(_thriftGenTransport);
            _myWriter = new ThriftCompactProtocolWriter(_myMs);
        }

        [Benchmark]
        public async Task WriteDecimalType_ThriftGen() {
            var dc = new DecimalTypeThriftGen {
                Precision = 10,
                Scale = 1
            };

            await dc.WriteAsync(_thriftGenProto, CancellationToken.None);
            byte[] buffer = _thriftGenTransport.GetBuffer();
        }

        [Benchmark]
        public async Task WriteDecimalType_MyGen() {
            var dc = new DecimalTypeMy {
                Precision = 10,
                Scale = 1 };

            dc.Write(_myWriter);
            byte[] buffer = _myMs.ToArray();
        }
    }
}
