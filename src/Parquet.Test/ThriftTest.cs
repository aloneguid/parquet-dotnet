﻿using System;
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

using CMThriftGen = Parquet.Thrift.ColumnMetaData;
using CMMy = Parquet.Meta.ColumnMetaData;
using Parquet.Meta;

namespace Parquet.Test {
    public class ThriftTest : TestBase {

        private readonly TMemoryBufferTransport _enTransport;
        private readonly TCompactProtocol _enProto;
        private readonly MemoryStream _myMs = new MemoryStream();
        private readonly ThriftCompactProtocolWriter _myWriter;

        public ThriftTest() {
            _enTransport = new TMemoryBufferTransport(null);
            _enProto = new TCompactProtocol(_enTransport);
            _myWriter = new ThriftCompactProtocolWriter(_myMs);
        }


        [Fact]
        public async Task Write_DecimalType() {
            // write with gen
            var dcGen = new DecimalTypeThriftGen {
                Precision = 10,
                Scale = 1
            };

            await dcGen.WriteAsync(_enProto, CancellationToken.None);
            byte[] bufferGen = _enTransport.GetBuffer();

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

            await dcGen.WriteAsync(_enProto, CancellationToken.None);
            byte[] bufferGen = _enTransport.GetBuffer();

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
        public async Task Read_statistics() {
            // write with gen
            var dcGen = new StatisticsThriftGen {
                Max = RandomGenerator.GetRandomBytes(3, 10),
                Min = RandomGenerator.GetRandomBytes(3, 10),
                Null_count = 4,
                Distinct_count = 400
            };
            dcGen.Max_value = dcGen.Max;
            dcGen.Min_value = dcGen.Min;

            await dcGen.WriteAsync(_enProto, CancellationToken.None);
            byte[] bufferGen = _enTransport.GetBuffer();

            // read with mine
            StatisticsMy dcMy = StatisticsMy.Read(new ThriftCompactProtocolReader(new MemoryStream(bufferGen)));
            Assert.Equal(dcGen.Max, dcMy.Max);
            Assert.Equal(dcGen.Min, dcMy.Min);
            Assert.Equal(dcGen.Null_count, dcMy.NullCount);
            Assert.Equal(dcGen.Distinct_count, dcMy.DistinctCount);
            Assert.Equal(dcGen.Max_value, dcMy.MaxValue);
            Assert.Equal(dcGen.Min_value, dcMy.MinValue);
        }

        [Fact]
        public async Task Write_Empty() {
            // write with gen
            var dcGen = new StringTypeThriftGen {
            };

            await dcGen.WriteAsync(_enProto, CancellationToken.None);
            byte[] bufferGen = _enTransport.GetBuffer();

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

            await dcGen.WriteAsync(_enProto, CancellationToken.None);
            byte[] bufferGen = _enTransport.GetBuffer();

            // write with my
            var dcMy = new IntTypeMy {
                BitWidth = 8,
                IsSigned = true
            };

            dcMy.Write(_myWriter);
            byte[] bufferMy = _myMs.ToArray();

            Assert.Equal(bufferGen, bufferMy);
        }

        [Fact]
        public async Task Write_Lists() {
            // write with gen
            var dcGen = new CMThriftGen {
                Encodings = new List<Thrift.Encoding> { Thrift.Encoding.PLAIN, Thrift.Encoding.PLAIN_DICTIONARY },
                Path_in_schema = new List<string> { "1", "22" }
            };

            await dcGen.WriteAsync(_enProto, CancellationToken.None);
            byte[] bufferGen = _enTransport.GetBuffer();

            // write with my
            var dcMy = new CMMy {
                Encodings = new List<Meta.Encoding> {  Meta.Encoding.PLAIN, Meta.Encoding.PLAIN_DICTIONARY },
                PathInSchema = new List<string> { "1", "22" }

            };

            dcMy.Write(_myWriter);
            byte[] bufferMy = _myMs.ToArray();

            Assert.Equal(bufferGen, bufferMy);
        }

        [Fact]
        public async Task Read_Lists() {
            // write with gen
            var dcGen = new CMThriftGen {
                Encodings = new List<Thrift.Encoding> { Thrift.Encoding.PLAIN, Thrift.Encoding.PLAIN_DICTIONARY },
                Path_in_schema = new List<string> { "1", "22" }
            };

            await dcGen.WriteAsync(_enProto, CancellationToken.None);
            byte[] bufferGen = _enTransport.GetBuffer();

            CMMy dcMy = CMMy.Read(new ThriftCompactProtocolReader(new MemoryStream(bufferGen)));

            Assert.Equal(new List<Parquet.Meta.Encoding> { Parquet.Meta.Encoding.PLAIN, Parquet.Meta.Encoding.PLAIN_DICTIONARY }, dcMy.Encodings);
            Assert.Equal(new List<string> { "1", "22" }, dcMy.PathInSchema);
        }

        [Fact]
        public async Task TestFileRead_Table() {
            using Stream fs = OpenTestFile("thrift/wide.bin");
            FileMetaData fileMeta = FileMetaData.Read(new ThriftCompactProtocolReader(fs));
        }
    }
}