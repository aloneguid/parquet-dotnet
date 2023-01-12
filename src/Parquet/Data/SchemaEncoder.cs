using System.Collections.Generic;
using Parquet.Schema;
using Parquet.Thrift;

namespace Parquet.Data {
    static class SchemaEncoder {

        class LookupItem {
            public Thrift.Type ThriftType { get; set; }

            public DataType? DefaultDataType { get; set; }

            public Dictionary<Thrift.ConvertedType, DataType> ConvertedTypes { get; set; }
        }

        class LookupTable : List<LookupItem> {

            private readonly Dictionary<Thrift.Type, DataType> _typeToDefaultType = new();
            private readonly Dictionary<KeyValuePair<Thrift.Type, Thrift.ConvertedType>, DataType> _typeAndConvertedTypeToType = new();

            public void Add(Thrift.Type thriftType, DataType dataType, params object[] options) {
                _typeToDefaultType.Add(thriftType, dataType);
                for(int i = 0; i < options.Length; i+=2) { 
                    var ct = (Thrift.ConvertedType)options[i];
                    var dt = (DataType)options[i+1];
                    _typeAndConvertedTypeToType.Add(new KeyValuePair<Thrift.Type, ConvertedType>(thriftType, ct), dt);
                }
            }

            public DataType? FindDataType(Thrift.SchemaElement se) {
                if(se.__isset.converted_type && 
                    _typeAndConvertedTypeToType.TryGetValue(new KeyValuePair<Thrift.Type, ConvertedType>(se.Type, se.Converted_type), out DataType match)) {
                    return match;
                }

                if(_typeToDefaultType.TryGetValue(se.Type, out match)) {
                    return match;
                }

                return null;
            }
        }

        private static readonly LookupTable LT = new LookupTable {
            { Thrift.Type.BOOLEAN, DataType.Boolean },
            { Thrift.Type.INT32, DataType.Int32,
                Thrift.ConvertedType.UINT_8, DataType.Byte,
                Thrift.ConvertedType.INT_8, DataType.SignedByte,
                Thrift.ConvertedType.UINT_16, DataType.UnsignedInt16,
                Thrift.ConvertedType.INT_16, DataType.Int16,
                Thrift.ConvertedType.UINT_32, DataType.UnsignedInt32,
                Thrift.ConvertedType.INT_32, DataType.Int32,
                Thrift.ConvertedType.DATE, DataType.DateTimeOffset,
                Thrift.ConvertedType.DECIMAL, DataType.Decimal,
                Thrift.ConvertedType.TIMESTAMP_MILLIS, DataType.DateTimeOffset
            },
            { Thrift.Type.INT64 , DataType.Int64,
                Thrift.ConvertedType.INT_64, DataType.Int64,
                Thrift.ConvertedType.UINT_64, DataType.UnsignedInt64,
                Thrift.ConvertedType.TIMESTAMP_MICROS, DataType.DateTimeOffset,
                Thrift.ConvertedType.DECIMAL, DataType.Decimal
            },
            { Thrift.Type.INT96, DataType.Int96 },
            { Thrift.Type.FLOAT, DataType.Float },
            { Thrift.Type.DOUBLE, DataType.Double },
            { Thrift.Type.BYTE_ARRAY, DataType.ByteArray,
                Thrift.ConvertedType.UTF8, DataType.String
            },
            { Thrift.Type.FIXED_LEN_BYTE_ARRAY, DataType.ByteArray,
                Thrift.ConvertedType.DECIMAL, DataType.Decimal,
                Thrift.ConvertedType.INTERVAL, DataType.Interval
            }
        };


        /// <summary>
        /// Builds <see cref="Field"/> from thrift schema
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="options"></param>
        /// <param name="index"></param>
        /// <param name="ownedChildCount"></param>
        /// <returns></returns>
        public static Field BuildField(List<Thrift.SchemaElement> schema,
            ParquetOptions options,
            ref int index, out int ownedChildCount) {

            Thrift.SchemaElement se = schema[index];
            bool hasNulls = se.Repetition_type != Thrift.FieldRepetitionType.REQUIRED;
            bool isArray = se.Repetition_type == Thrift.FieldRepetitionType.REPEATED;
            Field f = null;
            ownedChildCount = 0;

            DataType? dataType = LT.FindDataType(se);
            if(dataType != null) {
                if(options.TreatBigIntegersAsDates && dataType == DataType.Int96)
                    dataType = DataType.DateTimeOffset;

                if(options.TreatByteArrayAsString && dataType == DataType.ByteArray)
                    dataType = DataType.String;

                f = new DataField(se.Name, dataType.Value, hasNulls, isArray);
            }

            // todo: complex types (map, struct etc.)

            return f;
        }
    }
}