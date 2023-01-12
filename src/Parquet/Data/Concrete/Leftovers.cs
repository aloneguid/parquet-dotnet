using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using Parquet.File.Values.Primitives;
using Parquet.Schema;

// MIGRATION LEFTOVERS

namespace Parquet.Data.Concrete {

    class BooleanDataTypeHandler : BasicPrimitiveDataTypeHandler<bool> {
        public BooleanDataTypeHandler() : base(DataType.Boolean, Thrift.Type.BOOLEAN) { // T+
        }
    }

    class Int32DataTypeHandler : BasicPrimitiveDataTypeHandler<int> {
        public Int32DataTypeHandler() : base(DataType.Int32, Thrift.Type.INT32) { // T+
        }
    }

    class Int64DataTypeHandler : BasicPrimitiveDataTypeHandler<long> {
        public Int64DataTypeHandler() : base(DataType.Int64, Thrift.Type.INT64) { // T+
        }
    }

    class DoubleDataTypeHandler : BasicPrimitiveDataTypeHandler<double> {
        public DoubleDataTypeHandler() : base(DataType.Double, Thrift.Type.DOUBLE) {  // T+

        }
    }

    class FloatDataTypeHandler : BasicPrimitiveDataTypeHandler<float> {
        public FloatDataTypeHandler() : base(DataType.Float, Thrift.Type.FLOAT) { // T+
        }
    }

    class ByteArrayDataTypeHandler : BasicDataTypeHandler<byte[]> {
        private static readonly ArrayPool<byte[]> _byteArrayPool = ArrayPool<byte[]>.Shared;

        public ByteArrayDataTypeHandler() : base(DataType.ByteArray, Thrift.Type.BYTE_ARRAY) {  // T+
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
    }

    class ByteDataTypeHandler : BasicPrimitiveDataTypeHandler<byte> {
        public ByteDataTypeHandler() : base(DataType.Byte, Thrift.Type.INT32, Thrift.ConvertedType.UINT_8) { // T+

        }
    }

    class SignedByteDataTypeHandler : BasicPrimitiveDataTypeHandler<sbyte> {
        public SignedByteDataTypeHandler() : base(DataType.SignedByte, Thrift.Type.INT32, Thrift.ConvertedType.INT_8) { //T+

        }
    }

    class DateTimeDataTypeHandler : BasicPrimitiveDataTypeHandler<DateTime> {
        public DateTimeDataTypeHandler() : base(DataType.DateTimeOffset, Thrift.Type.BYTE_ARRAY) {

        }

        public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) => // T+
            (tse.Type == Thrift.Type.INT96 && formatOptions.TreatBigIntegersAsDates) || //Impala

            (tse.Type == Thrift.Type.INT64 && tse.__isset.converted_type &&
             tse.Converted_type is Thrift.ConvertedType.TIMESTAMP_MILLIS
                 or Thrift.ConvertedType.TIMESTAMP_MICROS) ||

            (tse.Type == Thrift.Type.INT32 && tse.__isset.converted_type &&
             tse.Converted_type == Thrift.ConvertedType.DATE);
    }

    class DateTimeOffsetDataTypeHandler : BasicPrimitiveDataTypeHandler<DateTimeOffset> {
        public DateTimeOffsetDataTypeHandler() : base(DataType.DateTimeOffset, Thrift.Type.INT96) {

        }

        public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) => // T+
            (tse.Type == Thrift.Type.INT96 && formatOptions.TreatBigIntegersAsDates) || //Impala

            (tse.Type == Thrift.Type.INT64 && tse.__isset.converted_type &&
             tse.Converted_type is Thrift.ConvertedType.TIMESTAMP_MILLIS
                 or Thrift.ConvertedType.TIMESTAMP_MICROS) ||

            (tse.Type == Thrift.Type.INT32 && tse.__isset.converted_type &&
             tse.Converted_type == Thrift.ConvertedType.DATE);

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

        public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) { // T+
            return tse.Type == Thrift.Type.INT96 && !formatOptions.TreatBigIntegersAsDates;
        }
    }

    class Int16DataTypeHandler : BasicPrimitiveDataTypeHandler<Int16> {
        public Int16DataTypeHandler() : base(DataType.Int16, Thrift.Type.INT32, Thrift.ConvertedType.INT_16) { // T+

        }
    }

    class UnsignedInt16DataTypeHandler : BasicPrimitiveDataTypeHandler<UInt16> {
        public UnsignedInt16DataTypeHandler() : base(DataType.UnsignedInt16, Thrift.Type.INT32, Thrift.ConvertedType.UINT_16) { // T+

        }
    }

    class DecimalDataTypeHandler : BasicPrimitiveDataTypeHandler<decimal> {
        public DecimalDataTypeHandler() : base(DataType.Decimal, Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.DECIMAL) {
        }

        public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) { // T+
            return

               tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.DECIMAL &&

               (
                  tse.Type == Thrift.Type.FIXED_LEN_BYTE_ARRAY ||
                  tse.Type == Thrift.Type.INT32 ||
                  tse.Type == Thrift.Type.INT64
               );
        }

        public override void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            base.CreateThrift(se, parent, container);

            //modify this element slightly
            Thrift.SchemaElement tse = container.Last();

            if(se is DecimalDataField dse) {
                if(dse.ForceByteArrayEncoding) {
                    tse.Type = Thrift.Type.FIXED_LEN_BYTE_ARRAY;
                }
                else {
                    if(dse.Precision <= 9)
                        tse.Type = Thrift.Type.INT32;
                    else if(dse.Precision <= 18)
                        tse.Type = Thrift.Type.INT64;
                    else
                        tse.Type = Thrift.Type.FIXED_LEN_BYTE_ARRAY;
                }

                tse.Precision = dse.Precision;
                tse.Scale = dse.Scale;
                tse.Type_length = BigDecimal.GetBufferSize(dse.Precision);
            }
            else {
                //set defaults
                tse.Precision = DecimalFormatDefaults.DefaultPrecision;
                tse.Scale = DecimalFormatDefaults.DefaultScale;
                tse.Type_length = 16;
            }
        }
    }

    class IntervalDataTypeHandler : BasicPrimitiveDataTypeHandler<Interval> {
        public IntervalDataTypeHandler() : base(DataType.Interval, Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.INTERVAL) { // T+

        }

        public override void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            base.CreateThrift(se, parent, container);

            //set type length to 12
            Thrift.SchemaElement tse = container.Last();
            tse.Type_length = 12;
        }
    }

    class TimeSpanDataTypeHandler : BasicPrimitiveDataTypeHandler<TimeSpan> {
        public TimeSpanDataTypeHandler() : base(DataType.TimeSpan, Thrift.Type.INT64, Thrift.ConvertedType.TIME_MICROS) { // T+

        }

        public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) {
            return

               (tse.Type == Thrift.Type.INT64 && tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.TIME_MICROS) ||

               (tse.Type == Thrift.Type.INT32 && tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.TIME_MILLIS);
        }

        public override void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            base.CreateThrift(se, parent, container);

            //modify annotations
            Thrift.SchemaElement tse = container.Last();
            if(se is TimeSpanDataField dse) {
                switch(dse.TimeSpanFormat) {
                    case TimeSpanFormat.MicroSeconds:
                        tse.Type = Thrift.Type.INT64;
                        tse.Converted_type = Thrift.ConvertedType.TIME_MICROS;
                        break;
                    case TimeSpanFormat.MilliSeconds:
                        tse.Type = Thrift.Type.INT32;
                        tse.Converted_type = Thrift.ConvertedType.TIME_MILLIS;
                        break;

                        //other cases are just default
                }
            }
            else {
                //default annotation is fine
            }

        }
    }

    class UnsignedInt32DataTypeHandler : BasicPrimitiveDataTypeHandler<uint> {
        public UnsignedInt32DataTypeHandler() : base(DataType.UnsignedInt32, Thrift.Type.INT32, Thrift.ConvertedType.UINT_32) { // T+

        }
    }

    class UnsignedInt64DataTypeHandler : BasicPrimitiveDataTypeHandler<ulong> {
        public UnsignedInt64DataTypeHandler() : base(DataType.UnsignedInt64, Thrift.Type.INT64, Thrift.ConvertedType.UINT_64) { // T+

        }
    }

    class StringDataTypeHandler : BasicDataTypeHandler<string> {
        private static readonly ArrayPool<string> _stringPool = ArrayPool<string>.Shared;

        public StringDataTypeHandler() : base(DataType.String, Thrift.Type.BYTE_ARRAY, Thrift.ConvertedType.UTF8) { // T+
        }

        public override Array GetArray(int minCount, bool rent, bool isNullable) {
            if(rent) {
                return _stringPool.Rent(minCount);
            }

            return new string[minCount];
        }

        public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) {
            return tse.__isset.type &&
               tse.Type == Thrift.Type.BYTE_ARRAY &&
               (
                  (tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.UTF8) ||
                  formatOptions.TreatByteArrayAsString
               );
        }

        public override ArrayView PackDefinitions(Array data, int offset, int count, int maxDefinitionLevel, out int[] definitions, out int definitionsLength, out int nullCount) {
            return PackDefinitions((string[])data, offset, count, maxDefinitionLevel, out definitions, out definitionsLength, out nullCount);
        }

        public override Array UnpackDefinitions(Array src, int[] definitionLevels, int maxDefinitionLevel) {
            return UnpackGenericDefinitions((string[])src, definitionLevels, maxDefinitionLevel);
        }

        public override int Compare(string x, string y) {
            return string.CompareOrdinal(x, y);
        }

        public override bool Equals(string x, string y) {
            return string.CompareOrdinal(x, y) == 0;
        }
    }
}
