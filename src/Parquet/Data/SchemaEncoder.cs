using System;
using System.Collections.Generic;
using System.Numerics;
using Parquet.File.Values.Primitives;
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
            private readonly Dictionary<DataType, System.Type> _dataTypeToSystemType = new();
            private readonly Dictionary<System.Type, DataType> _systemTypeToDataType = new();

            public void Add(Thrift.Type thriftType, DataType dataType, System.Type clrType, params object[] options) {
                _typeToDefaultType.Add(thriftType, dataType);
                _dataTypeToSystemType[dataType] = clrType;
                _systemTypeToDataType[clrType] = dataType;
                for(int i = 0; i < options.Length; i+=3) { 
                    var ct = (Thrift.ConvertedType)options[i];
                    var dt = (DataType)options[i+1];
                    var clr = (System.Type)options[i+2];
                    _typeAndConvertedTypeToType.Add(new KeyValuePair<Thrift.Type, ConvertedType>(thriftType, ct), dt);
                    _dataTypeToSystemType[dt] = clr;
                    _systemTypeToDataType[clr] = dt;
                }
            }

            public DataType? FindDataType(Thrift.SchemaElement se) {
                if(!se.__isset.type) return null;

                if(se.__isset.converted_type && 
                    _typeAndConvertedTypeToType.TryGetValue(new KeyValuePair<Thrift.Type, ConvertedType>(se.Type, se.Converted_type), out DataType match)) {
                    return match;
                }

                if(_typeToDefaultType.TryGetValue(se.Type, out match)) {
                    return match;
                }

                return null;
            }

            public DataType? FindDataType(System.Type type) {
                return _systemTypeToDataType.TryGetValue(type, out DataType match) ? match : null;
            }

            public System.Type FindSystemType(DataType dataType) {
                _dataTypeToSystemType.TryGetValue(dataType, out System.Type type);
                return type;
            }
        }

        private static readonly LookupTable LT = new LookupTable {
            { Thrift.Type.BOOLEAN, DataType.Boolean, typeof(bool) },
            { Thrift.Type.INT32, DataType.Int32, typeof(int),
                Thrift.ConvertedType.UINT_8, DataType.Byte, typeof(byte),
                Thrift.ConvertedType.INT_8, DataType.SignedByte, typeof(sbyte),
                Thrift.ConvertedType.UINT_16, DataType.UnsignedInt16, typeof(ushort),
                Thrift.ConvertedType.INT_16, DataType.Int16, typeof(short),
                Thrift.ConvertedType.UINT_32, DataType.UnsignedInt32, typeof(uint),
                Thrift.ConvertedType.INT_32, DataType.Int32, typeof(int),
                Thrift.ConvertedType.DATE, DataType.DateTimeOffset, typeof(DateTimeOffset),
                Thrift.ConvertedType.DECIMAL, DataType.Decimal, typeof(decimal),
                Thrift.ConvertedType.TIME_MILLIS, DataType.TimeSpan, typeof(TimeSpan),
                Thrift.ConvertedType.TIMESTAMP_MILLIS, DataType.DateTimeOffset, typeof(DateTimeOffset)
            },
            { Thrift.Type.INT64 , DataType.Int64, typeof(long),
                Thrift.ConvertedType.INT_64, DataType.Int64, typeof(long),
                Thrift.ConvertedType.UINT_64, DataType.UnsignedInt64, typeof(ulong),
                Thrift.ConvertedType.TIME_MICROS, DataType.TimeSpan, typeof(TimeSpan),
                Thrift.ConvertedType.TIMESTAMP_MICROS, DataType.DateTimeOffset, typeof(DateTimeOffset),
                Thrift.ConvertedType.TIMESTAMP_MILLIS, DataType.DateTimeOffset, typeof(DateTimeOffset),
                Thrift.ConvertedType.DECIMAL, DataType.Decimal, typeof(decimal)
            },
            { Thrift.Type.INT96, DataType.Int96, typeof(BigInteger) },
            { Thrift.Type.FLOAT, DataType.Float, typeof(float) },
            { Thrift.Type.DOUBLE, DataType.Double, typeof(double) },
            { Thrift.Type.BYTE_ARRAY, DataType.ByteArray, typeof(byte[]),
                Thrift.ConvertedType.UTF8, DataType.String, typeof(string)
            },
            { Thrift.Type.FIXED_LEN_BYTE_ARRAY, DataType.ByteArray, typeof(byte[]),
                Thrift.ConvertedType.DECIMAL, DataType.Decimal, typeof(decimal),
                Thrift.ConvertedType.INTERVAL, DataType.Interval, typeof(Interval)
            }
        };

        static bool TryBuildList(List<Thrift.SchemaElement> schema,
            ref int index, out int ownedChildren,
            out ListField field) {

            Thrift.SchemaElement se = schema[index];

            if(!(se.__isset.converted_type && se.Converted_type == Thrift.ConvertedType.LIST)) {
                ownedChildren = 0;
                field = null;
                return false;
            }

            Thrift.SchemaElement tseList = schema[index];
            field = ListField.CreateWithNoItem(tseList.Name);

            //https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
            Thrift.SchemaElement tseRepeated = schema[index + 1];

            //Rule 1. If the repeated field is not a group, then its type is the element type and elements are required.
            //not implemented

            //Rule 2. If the repeated field is a group with multiple fields, then its type is the element type and elements are required.
            //not implemented

            //Rule 3. f the repeated field is a group with one field and is named either array or uses
            //the LIST-annotated group's name with _tuple appended then the repeated type is the element
            //type and elements are required.

            // "group with one field and is named either array":
            if(tseList.Num_children == 1 && tseRepeated.Name == "array") {
                field.Path = tseList.Name;
                index += 1; //only skip this element
                ownedChildren = 1;
                return true;
            }

            //as we are skipping elements set path hint
            field.Path = new FieldPath(tseList.Name, schema[index + 1].Name);
            index += 2;          //skip this element and child container
            ownedChildren = 1; //we should get this element assigned back
            return true;
        }

        static bool TryBuildMap(List<Thrift.SchemaElement> schema,
            ref int index, out int ownedChildren,
            out MapField field) {

            Thrift.SchemaElement root = schema[index];
            bool isMap = root.__isset.converted_type &&
                (root.Converted_type == Thrift.ConvertedType.MAP || root.Converted_type == Thrift.ConvertedType.MAP_KEY_VALUE);
            if(!isMap) {
                ownedChildren= 0;
                field = null;
                return false;
            }

            //next element is a container
            Thrift.SchemaElement tseContainer = schema[++index];

            if(tseContainer.Num_children != 2) {
                throw new IndexOutOfRangeException($"dictionary container must have exactly 2 children but {tseContainer.Num_children} found");
            }

            //followed by a key and a value, but we declared them as owned

            var map = new MapField(root.Name);
            map.Path = new FieldPath(root.Name, tseContainer.Name);

            index += 1;
            ownedChildren = 2;
            field = map;
            return true;
        }

        static bool TryBuildStruct(List<Thrift.SchemaElement> schema,
            ref int index, out int ownedChildren,
            out StructField field) {
            Thrift.SchemaElement container = schema[index];
            bool isStruct = container.Num_children > 0;
            if(!isStruct) {
                ownedChildren = 0;
                field = null;
                return false;
            }

            index++;
            ownedChildren = container.Num_children; //make then owned to receive in .Assign()
            field = StructField.CreateWithNoElements(container.Name);
            return true;
        }

        /// <summary>
        /// Builds <see cref="Field"/> from thrift schema
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="options"></param>
        /// <param name="index"></param>
        /// <param name="ownedChildCount"></param>
        /// <returns></returns>
        public static Field Decode(List<Thrift.SchemaElement> schema,
            ParquetOptions options,
            ref int index, out int ownedChildCount) {

            Thrift.SchemaElement se = schema[index];
            bool hasNulls = se.Repetition_type != Thrift.FieldRepetitionType.REQUIRED;
            bool isArray = se.Repetition_type == Thrift.FieldRepetitionType.REPEATED;
            Field f = null;
            ownedChildCount = 0;

            DataType? dataType = LT.FindDataType(se);
            if(dataType != null) {
                // correction taking int account passed options
                if(options.TreatBigIntegersAsDates && dataType == DataType.Int96)
                    dataType = DataType.DateTimeOffset;

                if(options.TreatByteArrayAsString && dataType == DataType.ByteArray)
                    dataType = DataType.String;

                // successful field built
                f = new DataField(se.Name, dataType.Value, hasNulls, isArray);
                index++;
                return f;
            }

            if(TryBuildList(schema, ref index, out ownedChildCount, out ListField lf)) {
                f = lf;
            } else if(TryBuildMap(schema, ref index, out ownedChildCount, out MapField mf)) {
                f = mf;
            } else if(TryBuildStruct(schema, ref index, out ownedChildCount, out StructField sf)) {
                f = sf;
            }

            return f;
        }

        public static void Encode(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            if(se is DataField sef) {
                var tse = new Thrift.SchemaElement(se.Name);

                // find thrift type and converted type
                //tse.Type = _thriftType;
                //if(_convertedType != null)
                //    tse.Converted_type = _convertedType.Value;

                //bool isList = container.Count > 1 && container[container.Count - 2].Converted_type == Thrift.ConvertedType.LIST;

                //tse.Repetition_type = sef.IsArray && !isList
                //   ? Thrift.FieldRepetitionType.REPEATED
                //   : (sef.HasNulls ? Thrift.FieldRepetitionType.OPTIONAL : Thrift.FieldRepetitionType.REQUIRED);
                //container.Add(tse);
                //parent.Num_children += 1;
            }
        }

        /// <summary>
        /// Finds corresponding .NET type
        /// </summary>
        public static System.Type FindSystemType(DataType dataType) {
            return LT.FindSystemType(dataType);
        }

        public static DataType? FindDataType(System.Type type) {

            if(type == typeof(DateTime))
                type = typeof(DateTimeOffset);

            return LT.FindDataType(type);
        }
    }
}