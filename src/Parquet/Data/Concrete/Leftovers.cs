using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
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

    class DateTimeDataTypeHandler : BasicPrimitiveDataTypeHandler<DateTime> {
        public DateTimeDataTypeHandler() : base(DataType.DateTimeOffset, Thrift.Type.BYTE_ARRAY) {

        }

        public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) {
            return

               (tse.Type == Thrift.Type.INT96 && formatOptions.TreatBigIntegersAsDates) || //Impala

               (tse.Type == Thrift.Type.INT64 && tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.TIMESTAMP_MILLIS) ||

               (tse.Type == Thrift.Type.INT32 && tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.DATE);
        }
    }

    class DateTimeOffsetDataTypeHandler : BasicPrimitiveDataTypeHandler<DateTimeOffset> {
        public DateTimeOffsetDataTypeHandler() : base(DataType.DateTimeOffset, Thrift.Type.INT96) {

        }

        public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) {
            return

               (tse.Type == Thrift.Type.INT96 && formatOptions.TreatBigIntegersAsDates) || //Impala

               (tse.Type == Thrift.Type.INT64 && tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.TIMESTAMP_MILLIS) ||

               (tse.Type == Thrift.Type.INT32 && tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.DATE);
        }

        public override void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            base.CreateThrift(se, parent, container);

            //modify annotations
            Thrift.SchemaElement tse = container.Last();
            if(se is DateTimeDataField dse) {
                switch(dse.DateTimeFormat) {
                    case DateTimeFormat.DateAndTime:
                        tse.Type = Thrift.Type.INT64;
                        tse.Converted_type = Thrift.ConvertedType.TIMESTAMP_MILLIS;
                        break;
                    case DateTimeFormat.Date:
                        tse.Type = Thrift.Type.INT32;
                        tse.Converted_type = Thrift.ConvertedType.DATE;
                        break;

                        //other cases are just default
                }
            }
            else {
                tse.Converted_type = Thrift.ConvertedType.DATE;
                //default annotation is fine
            }

        }
    }

    class Int96DataTypeHandler : BasicPrimitiveDataTypeHandler<BigInteger> {
        public Int96DataTypeHandler() : base(DataType.Int96, Thrift.Type.INT96) {
        }

        public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) {
            return tse.Type == Thrift.Type.INT96 && !formatOptions.TreatBigIntegersAsDates;
        }
    }
}
