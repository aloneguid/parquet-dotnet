using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using Parquet.Data;
using Parquet.File.Values.Primitives;
using Parquet.Schema;
using Parquet.Thrift;
using SType = System.Type;
using Type = Parquet.Thrift.Type;

namespace Parquet.Encodings {
    static class SchemaEncoder {

        class LookupItem {
            public Thrift.Type ThriftType { get; set; }

            public SType? SystemType { get; set; }

            public Dictionary<Thrift.ConvertedType, SType>? ConvertedTypes { get; set; }
        }

        class LookupTable : List<LookupItem> {

            class SystemTypeInfo {
                public Thrift.Type tt;
                public Thrift.ConvertedType? tct;
                public int priority;
            }

            // all the cached lookups built during static initialisation
            private readonly HashSet<SType> _supportedTypes = new();
            private readonly Dictionary<Thrift.Type, SType> _typeToDefaultType = new();
            private readonly Dictionary<KeyValuePair<Thrift.Type, Thrift.ConvertedType>, SType> _typeAndConvertedTypeToType = new();
            private readonly Dictionary<SType, SystemTypeInfo> _systemTypeToTypeTuple = new();

            public void Add(Thrift.Type thriftType, SType t, params object[] options) {
                Add(thriftType, t, int.MaxValue, options);
            }

            private void AddSystemTypeInfo(Thrift.Type thriftType, SType t, Thrift.ConvertedType? ct, int priority) {
                if(_systemTypeToTypeTuple.TryGetValue(t, out SystemTypeInfo? sti)) {
                    if(priority <= sti.priority) {
                        sti.tt = thriftType;
                        sti.tct = ct;
                        sti.priority = priority;
                    }
                } else {
                    _systemTypeToTypeTuple[t] = new SystemTypeInfo {
                        tt = thriftType,
                        tct = ct,
                        priority = priority
                    };
                }
            }

            public void Add(Thrift.Type thriftType, SType t, int priority, params object[] options) {

                _typeToDefaultType[thriftType] = t;

                AddSystemTypeInfo(thriftType, t, null, priority);

                _supportedTypes.Add(t);
                for(int i = 0; i < options.Length; i += 2) {
                    var ct = (Thrift.ConvertedType)options[i];
                    var clr = (System.Type)options[i + 1];
                    _typeAndConvertedTypeToType.Add(new KeyValuePair<Thrift.Type, ConvertedType>(thriftType, ct), clr);

                    // more specific version overrides less specific
                    AddSystemTypeInfo(thriftType, clr, ct, int.MaxValue);

                    _supportedTypes.Add(clr);
                }
            }

            public SType? FindSystemType(Thrift.SchemaElement se) {
                if(!se.__isset.type)
                    return null;

                if(se.__isset.converted_type &&
                    _typeAndConvertedTypeToType.TryGetValue(
                        new KeyValuePair<Thrift.Type, ConvertedType>(se.Type, se.Converted_type),
                        out SType? match)) {
                    return match;
                }

                if(_typeToDefaultType.TryGetValue(se.Type, out match)) {
                    return match;
                }

                return null;
            }

            public bool FindTypeTuple(SType type, out Thrift.Type thriftType, out Thrift.ConvertedType? convertedType) {

                if(!_systemTypeToTypeTuple.TryGetValue(type, out SystemTypeInfo? sti)) {
                    thriftType = default;
                    convertedType = null;
                    return false;
                }

                thriftType = sti.tt;
                convertedType = sti.tct;
                return true;
            }

            public bool IsSupported(SType? t) => t != null && _supportedTypes.Contains(t);
        }

        [Obsolete]
        private static readonly Dictionary<SType, DataType> _systemTypeToObsoleteType = new() {
            { typeof(bool), DataType.Boolean },
            { typeof(byte), DataType.Byte },
            { typeof(sbyte), DataType.SignedByte },
            { typeof(short), DataType.Int16 },
            { typeof(ushort), DataType.UnsignedInt16 },
            { typeof(int), DataType.Int32 },
            { typeof(uint), DataType.UnsignedInt32 },
            { typeof(long), DataType.Int64 },
            { typeof(ulong), DataType.UnsignedInt64 },
            { typeof(BigInteger), DataType.Int96 },
            { typeof(byte[]), DataType.ByteArray },
            { typeof(string), DataType.String },
            { typeof(float), DataType.Float },
            { typeof(double), DataType.Double },
            { typeof(decimal), DataType.Decimal },
            { typeof(DateTime), DataType.DateTimeOffset },
            { typeof(Interval), DataType.Interval },
            { typeof(TimeSpan), DataType.TimeSpan }
        };

        private static readonly LookupTable _lt = new() {
            { Thrift.Type.BOOLEAN, typeof(bool) },
            { Thrift.Type.INT32, typeof(int),
                Thrift.ConvertedType.UINT_8, typeof(byte),
                Thrift.ConvertedType.INT_8, typeof(sbyte),
                Thrift.ConvertedType.UINT_16, typeof(ushort),
                Thrift.ConvertedType.INT_16, typeof(short),
                Thrift.ConvertedType.UINT_32, typeof(uint),
                Thrift.ConvertedType.INT_32, typeof(int),
                Thrift.ConvertedType.DATE, typeof(DateTime),
                Thrift.ConvertedType.DECIMAL, typeof(decimal),
                Thrift.ConvertedType.TIME_MILLIS, typeof(TimeSpan),
                Thrift.ConvertedType.TIMESTAMP_MILLIS, typeof(DateTime)
            },
            { Thrift.Type.INT64 , typeof(long),
                Thrift.ConvertedType.INT_64, typeof(long),
                Thrift.ConvertedType.UINT_64, typeof(ulong),
                Thrift.ConvertedType.TIME_MICROS, typeof(TimeSpan),
                Thrift.ConvertedType.TIMESTAMP_MICROS, typeof(DateTime),
                Thrift.ConvertedType.TIMESTAMP_MILLIS, typeof(DateTime),
                Thrift.ConvertedType.DECIMAL, typeof(decimal)
            },
            { Thrift.Type.INT96, typeof(DateTime), 1 },
            { Thrift.Type.INT96, typeof(BigInteger) },
            { Thrift.Type.FLOAT, typeof(float) },
            { Thrift.Type.DOUBLE, typeof(double) },
            { Thrift.Type.BYTE_ARRAY, typeof(byte[]), 1,
                Thrift.ConvertedType.UTF8, typeof(string),
                Thrift.ConvertedType.DECIMAL, typeof(decimal)
            },
            { Thrift.Type.FIXED_LEN_BYTE_ARRAY, typeof(byte[]),
                Thrift.ConvertedType.DECIMAL, typeof(decimal),
                Thrift.ConvertedType.INTERVAL, typeof(Interval)
            }
        };

        static bool TryBuildList(List<Thrift.SchemaElement> schema,
            ref int index, out int ownedChildren,
            out ListField? field) {

            Thrift.SchemaElement se = schema[index];

            if(!(se.__isset.converted_type && se.Converted_type == Thrift.ConvertedType.LIST)) {
                ownedChildren = 0;
                field = null;
                return false;
            }

            Thrift.SchemaElement tseList = schema[index];
            field = ListField.CreateWithNoItem(tseList.Name, tseList.Repetition_type != FieldRepetitionType.REQUIRED);

            //https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
            Thrift.SchemaElement tseRepeated = schema[index + 1];

            // Rule 1. If the repeated field is not a group, then its type is the element type and elements are required.
            // todo: not implemented

            // Rule 2. If the repeated field is a group with multiple fields, then its type is the element type and elements are required.
            // todo: not implemented

            // Rule 3. If the repeated field is a group with one field and is named either "array" or uses
            // the "LIST"-annotated group's name with "_tuple" appended then the repeated type is the element
            // type and elements are required.
            // todo: not implemented fully, only "array"

            // "group with one field and is named either array":
            if(tseList.Num_children == 1 && tseRepeated.Name == "array") {
                field.Path = tseList.Name;
                index += 1; //only skip this element
                ownedChildren = 1;
                return true;
            }

            // Normal "modern" LIST:
            //as we are skipping elements set path hint
            Thrift.SchemaElement tseRepeatedGroup = schema[index + 1];
            field.Path = new FieldPath(tseList.Name, tseRepeatedGroup.Name);
            field.GroupSchemaElement = tseRepeatedGroup;
            index += 2;          //skip this element and child container
            ownedChildren = 1;   //we should get this element assigned back
            return true;
        }

        static bool TryBuildMap(List<Thrift.SchemaElement> schema,
            ref int index, out int ownedChildren,
            out MapField? field) {

            Thrift.SchemaElement root = schema[index];
            bool isMap = root.__isset.converted_type &&
                (root.Converted_type == Thrift.ConvertedType.MAP || root.Converted_type == Thrift.ConvertedType.MAP_KEY_VALUE);
            if(!isMap) {
                ownedChildren = 0;
                field = null;
                return false;
            }

            //next element is a container
            Thrift.SchemaElement tseContainer = schema[++index];

            if(tseContainer.Num_children != 2) {
                throw new IndexOutOfRangeException($"dictionary container must have exactly 2 children but {tseContainer.Num_children} found");
            }

            //followed by a key and a value, but we declared them as owned

            var map = new MapField(root.Name) {
                Path = new FieldPath(root.Name, tseContainer.Name),
                IsNullable = root.Repetition_type != FieldRepetitionType.REQUIRED,
                GroupSchemaElement = tseContainer
            };

            index += 1;
            ownedChildren = 2;
            field = map;
            return true;
        }

        static bool TryBuildStruct(List<Thrift.SchemaElement> schema,
            ref int index, out int ownedChildren,
            out StructField? field) {
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
            field.IsNullable = container.Repetition_type != FieldRepetitionType.REQUIRED;
            return true;
        }

        public static bool IsSupported(SType? t) => t == typeof(DateTime) || _lt.IsSupported(t);

        /// <summary>
        /// Builds <see cref="Field"/> from thrift schema
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="options"></param>
        /// <param name="index"></param>
        /// <param name="ownedChildCount"></param>
        /// <returns></returns>
        public static Field? Decode(List<Thrift.SchemaElement> schema,
            ParquetOptions? options,
            ref int index, out int ownedChildCount) {

            Thrift.SchemaElement se = schema[index];
            bool isNullable = se.Repetition_type != Thrift.FieldRepetitionType.REQUIRED;
            bool isArray = se.Repetition_type == Thrift.FieldRepetitionType.REPEATED;
            Field? f = null;
            ownedChildCount = 0;

            SType? t = _lt.FindSystemType(se);
            if(t != null) {
                // correction taking int account passed options
                if(options != null && options.TreatBigIntegersAsDates && t == typeof(BigInteger))
                    t = typeof(DateTime);

                if(options != null && options.TreatByteArrayAsString && t == typeof(byte[]))
                    t = typeof(string);

                DataField? df;
                if(t == typeof(DateTime)) {
                    df = GetDateTimeDataField(se);
                } 
                else{
                    // successful field built
                    df = new DataField(se.Name, t);
                }

                df.IsNullable = isNullable;
                df.IsArray = isArray;
                f = df;

                index++;
                return f;
            }

            if(TryBuildList(schema, ref index, out ownedChildCount, out ListField? lf)) {
                f = lf;
            } else if(TryBuildMap(schema, ref index, out ownedChildCount, out MapField? mf)) {
                f = mf;
            } else if(TryBuildStruct(schema, ref index, out ownedChildCount, out StructField? sf)) {
                f = sf;
            }

            return f;
        }

        private static DataField GetDateTimeDataField(SchemaElement se)
        {
            switch (se.Converted_type)
            {
                case ConvertedType.TIMESTAMP_MILLIS:
                    if (se.Type == Type.INT64)
                        return new DateTimeDataField(se.Name, DateTimeFormat.DateAndTime);
                    break;
                case ConvertedType.DATE:
                    if(se.Type == Type.INT32)
                        return new DateTimeDataField(se.Name, DateTimeFormat.Date);
                    break;
            }
            return new DateTimeDataField(se.Name, DateTimeFormat.Impala);
        }

        /// <summary>
        /// Adjust type-specific schema encodings that do not always follow generic use case
        /// </summary>
        /// <param name="df"></param>
        /// <param name="tse"></param>
        private static void AdjustEncoding(DataField df, Thrift.SchemaElement tse) {
            if(df.ClrType == typeof(DateTime)) {
                if(df is DateTimeDataField dfDateTime) {
                    switch(dfDateTime.DateTimeFormat) {
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
            } else if(df.ClrType == typeof(decimal)) {
                if(df is DecimalDataField dfDecimal) {
                    if(dfDecimal.ForceByteArrayEncoding)
                        tse.Type = Thrift.Type.FIXED_LEN_BYTE_ARRAY;
                    else if(dfDecimal.Precision <= 9)
                        tse.Type = Thrift.Type.INT32;
                    else if(dfDecimal.Precision <= 18)
                        tse.Type = Thrift.Type.INT64;
                    else
                        tse.Type = Thrift.Type.FIXED_LEN_BYTE_ARRAY;

                    tse.Precision = dfDecimal.Precision;
                    tse.Scale = dfDecimal.Scale;
                    tse.Type_length = BigDecimal.GetBufferSize(dfDecimal.Precision);
                } else {
                    //set defaults
                    tse.Precision = DecimalFormatDefaults.DefaultPrecision;
                    tse.Scale = DecimalFormatDefaults.DefaultScale;
                    tse.Type_length = 16;
                }
            } else if(df.ClrType == typeof(Interval)) {
                //set type length to 12
                tse.Type_length = 12;
            } else if(df.ClrType == typeof(TimeSpan)) {
                if(df is TimeSpanDataField dfTime) {
                    switch(dfTime.TimeSpanFormat) {
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
            }
        }

        private static void Encode(ListField listField, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            parent.Num_children += 1;

            //add list container
            var root = new Thrift.SchemaElement(listField.Name) {
                Converted_type = Thrift.ConvertedType.LIST,
                Repetition_type = listField.IsNullable ? Thrift.FieldRepetitionType.OPTIONAL : Thrift.FieldRepetitionType.REQUIRED,
                Num_children = 1  //field container below
            };
            container.Add(root);

            //add field container
            var list = new Thrift.SchemaElement(listField.ContainerName) {
                Repetition_type = Thrift.FieldRepetitionType.REPEATED
            };
            container.Add(list);

            //add the list item as well
            Encode(listField.Item, list, container);
        }

        private static void Encode(MapField mapField, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            parent.Num_children += 1;

            //add the root container where map begins
            var root = new Thrift.SchemaElement(mapField.Name) {
                Converted_type = Thrift.ConvertedType.MAP,
                Num_children = 1,
                Repetition_type = Thrift.FieldRepetitionType.OPTIONAL
            };
            container.Add(root);

            //key-value is a container for column of keys and column of values
            var keyValue = new Thrift.SchemaElement(MapField.ContainerName) {
                Num_children = 0, //is assigned by children
                Repetition_type = Thrift.FieldRepetitionType.REPEATED
            };
            container.Add(keyValue);

            //now add the key and value separately
            Encode(mapField.Key, keyValue, container);
            Thrift.SchemaElement tseKey = container[container.Count - 1];
            Encode(mapField.Value, keyValue, container);
            Thrift.SchemaElement tseValue = container[container.Count - 1];

            //fixes for weirdness in RLs
            if(tseKey.Repetition_type == Thrift.FieldRepetitionType.REPEATED)
                tseKey.Repetition_type = Thrift.FieldRepetitionType.OPTIONAL;
            if(tseValue.Repetition_type == Thrift.FieldRepetitionType.REPEATED)
                tseValue.Repetition_type = Thrift.FieldRepetitionType.OPTIONAL;
        }

        private static void Encode(StructField structField, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            var tseStruct = new Thrift.SchemaElement(structField.Name) {
                Repetition_type = Thrift.FieldRepetitionType.OPTIONAL,
            };
            container.Add(tseStruct);
            parent.Num_children += 1;

            foreach(Field memberField in structField.Fields) {
                Encode(memberField, tseStruct, container);
            }
        }

        public static void Encode(Field field, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            if(field.SchemaType == SchemaType.Data && field is DataField dataField) {
                var tse = new Thrift.SchemaElement(field.Name);

                if(!_lt.FindTypeTuple(dataField.ClrType, out Thrift.Type thriftType, out Thrift.ConvertedType? convertedType)) {
                    throw new NotSupportedException($"could not find type tuple for {dataField.ClrType}");
                }

                tse.Type = thriftType;
                if(convertedType != null) {
                    // be careful calling thrift setter as it sets other hidden flags
                    tse.Converted_type = convertedType.Value;
                }

                bool isList = container.Count > 1 && container[container.Count - 2].Converted_type == Thrift.ConvertedType.LIST;

                tse.Repetition_type = dataField.IsArray && !isList
                   ? Thrift.FieldRepetitionType.REPEATED
                   : (dataField.IsNullable ? Thrift.FieldRepetitionType.OPTIONAL : Thrift.FieldRepetitionType.REQUIRED);
                container.Add(tse);
                parent.Num_children += 1;

                AdjustEncoding(dataField, tse);
            } else if(field.SchemaType == SchemaType.List && field is ListField listField) {
                Encode(listField, parent, container);
            } else if(field.SchemaType == SchemaType.Map && field is MapField mapField) {
                Encode(mapField, parent, container);
            } else if(field.SchemaType == SchemaType.Struct && field is StructField structField) {
                Encode(structField, parent, container);
            } else {
                throw new InvalidOperationException($"unable to encode {field}");
            }
        }

        public static bool FindTypeTuple(SType type, out Thrift.Type thriftType, out Thrift.ConvertedType? convertedType) =>
            _lt.FindTypeTuple(type, out thriftType, out convertedType);

        /// <summary>
        /// Finds corresponding .NET type
        /// </summary>
        [Obsolete]
        public static SType? FindSystemType(DataType dataType) =>
            (from pair in _systemTypeToObsoleteType where pair.Value == dataType select pair.Key).FirstOrDefault();

        [Obsolete]
        public static DataType? FindDataType(SType type) => _systemTypeToObsoleteType[type];
    }
}