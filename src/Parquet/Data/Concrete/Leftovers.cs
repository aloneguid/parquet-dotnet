using System;
using System.Buffers;
using System.IO;
using Parquet.Schema;

// MIGRATION LEFTOVERS

namespace Parquet.Data.Concrete {

    class BooleanDataTypeHandler : BasicPrimitiveDataTypeHandler<bool> {
        public BooleanDataTypeHandler() : base(DataType.Boolean, Thrift.Type.BOOLEAN) {
        }
    }

    class Int32DataTypeHandler : BasicPrimitiveDataTypeHandler<int> {
        public Int32DataTypeHandler() : base(DataType.Int32, Thrift.Type.INT32) {
        }
    }

    class Int64DataTypeHandler : BasicPrimitiveDataTypeHandler<long> {
        public Int64DataTypeHandler() : base(DataType.Int64, Thrift.Type.INT64) {
        }
    }

    class DoubleDataTypeHandler : BasicPrimitiveDataTypeHandler<double> {
        public DoubleDataTypeHandler() : base(DataType.Double, Thrift.Type.DOUBLE) {

        }
    }

    class FloatDataTypeHandler : BasicPrimitiveDataTypeHandler<float> {
        public FloatDataTypeHandler() : base(DataType.Float, Thrift.Type.FLOAT) {
        }
    }

    class ByteArrayDataTypeHandler : BasicDataTypeHandler<byte[]> {
        private static readonly ArrayPool<byte[]> _byteArrayPool = ArrayPool<byte[]>.Shared;

        public ByteArrayDataTypeHandler() : base(DataType.ByteArray, Thrift.Type.BYTE_ARRAY) {
        }

        public override Array GetArray(int minCount, bool rent, bool isNullable) {
            if(rent) {
                return _byteArrayPool.Rent(minCount);
            }

            return new byte[minCount][];
        }

        public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) {
            return tse.__isset.type && tse.Type == Thrift.Type.BYTE_ARRAY
                                    && !tse.__isset.converted_type;
        }

        public override ArrayView PackDefinitions(Array data, int offset, int count, int maxDefinitionLevel, out int[] definitions, out int definitionsLength, out int nullCount) {
            return PackDefinitions((byte[][])data, offset, count, maxDefinitionLevel, out definitions, out definitionsLength, out nullCount);
        }

        public override Array UnpackDefinitions(Array src, int[] definitionLevels, int maxDefinitionLevel) {
            return UnpackGenericDefinitions((byte[][])src, definitionLevels, maxDefinitionLevel);
        }

        public override int Compare(byte[] x, byte[] y) {
            return 0;
        }

        public override bool Equals(byte[] x, byte[] y) {
            return x == y;
        }

        public override object PlainDecode(Thrift.SchemaElement tse, byte[] encoded) {
            return encoded;
        }
    }

    class ByteDataTypeHandler : BasicPrimitiveDataTypeHandler<byte> {
        public ByteDataTypeHandler() : base(DataType.Byte, Thrift.Type.INT32, Thrift.ConvertedType.UINT_8) {

        }
    }

    class SignedByteDataTypeHandler : BasicPrimitiveDataTypeHandler<sbyte> {
        public SignedByteDataTypeHandler() : base(DataType.SignedByte, Thrift.Type.INT32, Thrift.ConvertedType.INT_8) {

        }
    }
}
