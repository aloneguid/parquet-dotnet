using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Thrift.Protocol;
using Thrift.Transport.Client;
using Parquet.Meta.Proto;
using System.IO;
using System.Threading;
using NetBox.Generator;


using DecimalTypeThriftGen = Parquet.Thrift.DecimalType;
using DecimalTypeMy = Parquet.Meta.DecimalType;

using StatisticsThriftGen = Parquet.Thrift.Statistics;
using StatisticsMy = Parquet.Meta.Statistics;

using StringTypeThriftGen = Parquet.Thrift.StringType;
using StringTypeMy = Parquet.Meta.StringType;

using IntTypeThriftGen = Parquet.Thrift.IntType;
using IntTypeMy = Parquet.Meta.IntType;

namespace Parquet.Test {
    public class ThriftTest {

        private readonly TMemoryBufferTransport _thriftGenTransport;
        private readonly TCompactProtocol _thriftGenProto;
        private readonly MemoryStream _myMs = new MemoryStream();
        private readonly ThriftCompactProtocolWriter _myWriter;

        public ThriftTest() {
            _thriftGenTransport = new TMemoryBufferTransport(null);
            _thriftGenProto = new TCompactProtocol(_thriftGenTransport);
            _myWriter = new ThriftCompactProtocolWriter(_myMs);
        }


        [Fact]
        public async Task Write_DecimalType() {
            // write with gen
            var dcGen = new DecimalTypeThriftGen {
                Precision = 10,
                Scale = 1
            };

            await dcGen.WriteAsync(_thriftGenProto, CancellationToken.None);
            byte[] bufferGen = _thriftGenTransport.GetBuffer();

            // write with my
            var dcMy = new DecimalTypeMy {
                Precision = 10,
                Scale = 1
            };

            dcMy.Write(_myWriter);
            byte[] bufferMy = _myMs.ToArray();

            Assert.Equal(bufferGen, bufferMy);
        }

        [Fact]
        public async Task Write_Statistics() {
            // write with gen
            var dcGen = new StatisticsThriftGen {
                Max = RandomGenerator.GetRandomBytes(3, 10),
                Min = RandomGenerator.GetRandomBytes(3, 10),
                Null_count = 4,
                Distinct_count = 400
            };
            dcGen.Max_value = dcGen.Max;
            dcGen.Min_value = dcGen.Min;

            await dcGen.WriteAsync(_thriftGenProto, CancellationToken.None);
            byte[] bufferGen = _thriftGenTransport.GetBuffer();

            // write with my
            var dcMy = new StatisticsMy {
                Max = dcGen.Max,
                Min = dcGen.Min,
                NullCount = dcGen.Null_count,
                DistinctCount = dcGen.Distinct_count,
                MaxValue = dcGen.Max,
                MinValue = dcGen.Min
            };

            dcMy.Write(_myWriter);
            byte[] bufferMy = _myMs.ToArray();

            Assert.Equal(bufferGen, bufferMy);
        }

        [Fact]
        public async Task Write_Empty() {
            // write with gen
            var dcGen = new StringTypeThriftGen {
            };

            await dcGen.WriteAsync(_thriftGenProto, CancellationToken.None);
            byte[] bufferGen = _thriftGenTransport.GetBuffer();

            // write with my
            var dcMy = new StringTypeMy {
            };

            dcMy.Write(_myWriter);
            byte[] bufferMy = _myMs.ToArray();

            Assert.Equal(bufferGen, bufferMy);
        }

        [Fact]
        public async Task Write_Bool() {
            // write with gen
            var dcGen = new IntTypeThriftGen {
                BitWidth = 8,
                IsSigned = true
            };

            await dcGen.WriteAsync(_thriftGenProto, CancellationToken.None);
            byte[] bufferGen = _thriftGenTransport.GetBuffer();

            // write with my
            var dcMy = new IntTypeMy {
                BitWidth = 8,
                IsSigned = true
            };

            dcMy.Write(_myWriter);
            byte[] bufferMy = _myMs.ToArray();

            Assert.Equal(bufferGen, bufferMy);
        }
    }
}
