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

        public static readonly SType[] SupportedTypes = new[] {
            typeof(bool),
            typeof(byte), typeof(sbyte),
            typeof(short), typeof(ushort),
            typeof(int), typeof(uint),
            typeof(long), typeof(ulong),
            typeof(float),
            typeof(double),
            typeof(decimal),
            typeof(BigInteger),
            typeof(DateTime),
            typeof(TimeSpan),
            typeof(Interval),
            typeof(byte[]),
            typeof(string)
        };

        static bool TryBuildList(List<Thrift.SchemaElement> schema,
            ref int index, out int ownedChildren,
            out ListField? field) {

            Thrift.SchemaElement outerGroup = schema[index];

            if(!(outerGroup.__isset.converted_type && outerGroup.Converted_type == Thrift.ConvertedType.LIST)) {
                ownedChildren = 0;
                field = null;
                return false;
            }

            field = ListField.CreateWithNoItem(outerGroup.Name, outerGroup.Repetition_type != FieldRepetitionType.REQUIRED);

            //https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
            Thrift.SchemaElement midGroup = schema[index + 1];
            bool midIsGroup = midGroup.Num_children > 0;

            // Rule 1. If the repeated field is not a group, then its type is the element type and elements are required.
            if(!midIsGroup) {
                field.Path = new FieldPath(outerGroup.Name);
                field.ThriftSchemaElement = outerGroup;
                index += 1; //only skip this element
                ownedChildren = 1;  // next element is list's item
                return true;
            }

            // Rule 2. If the repeated field is a group with multiple fields, then its type is the element type and elements are required.
            // todo: not implemented

            // Rule 3. If the repeated field is a group with one field and is named either "array" or uses
            // the "LIST"-annotated group's name with "_tuple" appended then the repeated type is the element
            // type and elements are required.
            // todo: not implemented fully, only "array"

            // Normal "modern" LIST:
            // as we are skipping elements set path hint
            field.Path = new FieldPath(outerGroup.Name, midGroup.Name);
            field.ThriftSchemaElement = outerGroup;
            field.GroupSchemaElement = midGroup;
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
                GroupSchemaElement = tseContainer,
                ThriftSchemaElement = root
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
            field.ThriftSchemaElement = container;
            return true;
        }

        public static bool IsSupported(SType? t) => SupportedTypes.Contains(t);

        /// <summary>
        /// Builds <see cref="Field"/> from thrift schema
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="options"></param>
        /// <param name="index"></param>
        /// <param name="ownedChildCount"></param>
        /// <returns></returns>
        public static Field? Decode(List<Thrift.SchemaElement> schema,
            ParquetOptions options,
            ref int index, out int ownedChildCount) {

            Thrift.SchemaElement se = schema[index];
            bool isNullable = se.Repetition_type != Thrift.FieldRepetitionType.REQUIRED;
            bool isArray = se.Repetition_type == Thrift.FieldRepetitionType.REPEATED;
            Field? f = null;
            ownedChildCount = 0;

            if(TryBuildDataField(se, options, out DataField? df)) {
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

        private static bool TryBuildDataField(SchemaElement se, ParquetOptions options, out DataField? df) {
            df = null;

            if(!se.__isset.type)
                return false;

            SType? st = se.Type switch {
                Thrift.Type.BOOLEAN => typeof(bool),

                Thrift.Type.INT32 when se.__isset.converted_type => se.Converted_type switch {
                    Thrift.ConvertedType.INT_8 => typeof(sbyte),
                    Thrift.ConvertedType.UINT_8 => typeof(byte),
                    Thrift.ConvertedType.INT_16 => typeof(short),
                    Thrift.ConvertedType.UINT_16 => typeof(ushort),
                    Thrift.ConvertedType.INT_32 => typeof(int),
                    Thrift.ConvertedType.UINT_32 => typeof(uint),
                    Thrift.ConvertedType.DATE => typeof(DateTime),
                    Thrift.ConvertedType.DECIMAL => typeof(decimal),
                    Thrift.ConvertedType.TIME_MILLIS => typeof(TimeSpan),
                    Thrift.ConvertedType.TIMESTAMP_MILLIS => typeof(DateTime),
                    _ => typeof(int)
                },
                Thrift.Type.INT32 => typeof(int),

                Thrift.Type.INT64 when se.__isset.converted_type => se.Converted_type switch {
                    Thrift.ConvertedType.INT_64 => typeof(long),
                    Thrift.ConvertedType.UINT_64 => typeof(ulong),
                    Thrift.ConvertedType.TIME_MICROS => typeof(TimeSpan),
                    Thrift.ConvertedType.TIMESTAMP_MICROS => typeof(DateTime),
                    Thrift.ConvertedType.TIMESTAMP_MILLIS => typeof(DateTime),
                    Thrift.ConvertedType.DECIMAL => typeof(decimal),
                    _ => typeof(long)
                },
                Thrift.Type.INT64 => typeof(long),

                Thrift.Type.INT96 when options.TreatBigIntegersAsDates => typeof(DateTime),
                Thrift.Type.INT96 => typeof(BigInteger),
                Thrift.Type.FLOAT => typeof(float),
                Thrift.Type.DOUBLE => typeof(double),

                Thrift.Type.BYTE_ARRAY when se.__isset.converted_type => se.Converted_type switch {
                    Thrift.ConvertedType.UTF8 => typeof(string),
                    Thrift.ConvertedType.DECIMAL => typeof(decimal),
                    _ => typeof(byte[])
                },
                Thrift.Type.BYTE_ARRAY => options.TreatByteArrayAsString ? typeof(string) : typeof(byte[]),

                Thrift.Type.FIXED_LEN_BYTE_ARRAY when se.__isset.converted_type => se.Converted_type switch {
                    Thrift.ConvertedType.DECIMAL => typeof(decimal),
                    Thrift.ConvertedType.INTERVAL => typeof(Interval),
                    _ => typeof(byte[])
                },
                Thrift.Type.FIXED_LEN_BYTE_ARRAY => options.TreatByteArrayAsString ? typeof(string) : typeof(byte[]),

                _ => null
            };

            if(st == null)
                return false;

            if(st == typeof(DateTime)) {
                df = GetDateTimeDataField(se);
            } else {
                // successful field built
                df = new DataField(se.Name, st);
            }
            bool isNullable = se.Repetition_type != Thrift.FieldRepetitionType.REQUIRED;
            bool isArray = se.Repetition_type == Thrift.FieldRepetitionType.REPEATED;
            df.IsNullable = isNullable;
            df.IsArray = isArray;
            df.ThriftSchemaElement = se;
            return true;
        }

        private static DataField GetDateTimeDataField(SchemaElement se) {
            switch(se.Converted_type) {
                case ConvertedType.TIMESTAMP_MILLIS:
                    if(se.Type == Type.INT64)
                        return new DateTimeDataField(se.Name, DateTimeFormat.DateAndTime);
                    break;
                case ConvertedType.DATE:
                    if(se.Type == Type.INT32)
                        return new DateTimeDataField(se.Name, DateTimeFormat.Date);
                    break;
            }
            return new DateTimeDataField(se.Name, DateTimeFormat.Impala);
        }

        private static void Encode(ListField listField, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            parent.Num_children += 1;

            //add list container
            var root = new Thrift.SchemaElement(listField.Name) {
                Converted_type = Thrift.ConvertedType.LIST,
                LogicalType = new Thrift.LogicalType { LIST = new Thrift.ListType() },
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
                LogicalType = new Thrift.LogicalType { MAP = new Thrift.MapType() },
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
                Repetition_type = Thrift.FieldRepetitionType.OPTIONAL
                // no logical or converted type annotations for structs
            };
            container.Add(tseStruct);
            parent.Num_children += 1;

            foreach(Field memberField in structField.Fields) {
                Encode(memberField, tseStruct, container);
            }
        }

        public static Thrift.SchemaElement Encode(DataField field) {
            SType st = field.ClrType;
            var tse = new Thrift.SchemaElement(field.Name);
            tse.LogicalType = new Thrift.LogicalType();

            if(st == typeof(bool)) {                                // boolean
                tse.Type = Thrift.Type.BOOLEAN;
            } else if(st == typeof(byte) || st == typeof(sbyte) ||  // 32-bit numbers
                st == typeof(short) || st == typeof(ushort) ||
                st == typeof(int) || st == typeof(uint)) {

                tse.Type = Thrift.Type.INT32;
                sbyte bw = 0;
                if(st == typeof(byte) || st == typeof(sbyte))
                    bw = 8;
                else if(st == typeof(short) || st == typeof(ushort))
                    bw = 16;
                else if(st == typeof(int) || st == typeof(uint))
                    bw = 32;
                bool signed = st == typeof(sbyte) || st == typeof(short) || st == typeof(int);

                tse.LogicalType.INTEGER = new Thrift.IntType {
                    BitWidth = bw,
                    IsSigned = signed
                };
                tse.Converted_type = bw switch {
                    8 => signed ? Thrift.ConvertedType.INT_8 : Thrift.ConvertedType.UINT_8,
                    16 => signed ? Thrift.ConvertedType.INT_16 : Thrift.ConvertedType.UINT_16,
                    32 => signed ? Thrift.ConvertedType.INT_32 : Thrift.ConvertedType.UINT_32,
                    _ => Thrift.ConvertedType.INT_32
                };
            } else if(st == typeof(long) || st == typeof(ulong)) {  // 64-bit number
                tse.Type = Thrift.Type.INT64;
                tse.LogicalType.INTEGER = new Thrift.IntType { BitWidth = 64, IsSigned = st == typeof(long) };
                tse.Converted_type = st == typeof(long) ? Thrift.ConvertedType.INT_64 : Thrift.ConvertedType.UINT_64;
            } else if(st == typeof(float)) {                        // float
                tse.Type = Thrift.Type.FLOAT;
            } else if(st == typeof(double)) {                       // double
                tse.Type = Thrift.Type.DOUBLE;
            } else if(st == typeof(BigInteger)) {                   // BigInteger
                tse.Type = Thrift.Type.INT96;
            } else if(st == typeof(string)) {                       // string
                tse.Type = Thrift.Type.BYTE_ARRAY;
                tse.LogicalType.STRING = new Thrift.StringType();
                tse.Converted_type = Thrift.ConvertedType.UTF8;
            } else if(st == typeof(decimal)) {                      // decimal

                int precision;
                int scale;

                if(field is DecimalDataField dfDecimal) {
                    if(dfDecimal.ForceByteArrayEncoding)
                        tse.Type = Thrift.Type.FIXED_LEN_BYTE_ARRAY;
                    else if(dfDecimal.Precision <= 9)
                        tse.Type = Thrift.Type.INT32;
                    else if(dfDecimal.Precision <= 18)
                        tse.Type = Thrift.Type.INT64;
                    else
                        tse.Type = Thrift.Type.FIXED_LEN_BYTE_ARRAY;

                    precision = dfDecimal.Precision;
                    scale = dfDecimal.Scale;
                    tse.Type_length = BigDecimal.GetBufferSize(dfDecimal.Precision);
                } else {
                    //set defaults
                    tse.Type = Thrift.Type.FIXED_LEN_BYTE_ARRAY;
                    precision = DecimalFormatDefaults.DefaultPrecision;
                    scale = DecimalFormatDefaults.DefaultScale;
                    tse.Type_length = 16;
                }

                tse.LogicalType.DECIMAL = new Thrift.DecimalType {
                    Precision = precision,
                    Scale = scale
                };
                tse.Precision = precision;
                tse.Scale = scale;
            } else if(st == typeof(byte[])) {           // byte[]
                tse.Type = Thrift.Type.BYTE_ARRAY;
            } else if(st == typeof(DateTime)) {         // DateTime
                if(field is DateTimeDataField dfDateTime) {
                    switch(dfDateTime.DateTimeFormat) {
                        case DateTimeFormat.DateAndTime:
                            tse.Type = Thrift.Type.INT64;
                            tse.Converted_type = Thrift.ConvertedType.TIMESTAMP_MILLIS;
                            break;
                        case DateTimeFormat.Date:
                            tse.Type = Thrift.Type.INT32;
                            tse.Converted_type = Thrift.ConvertedType.DATE;
                            break;
                        default:
                            tse.Type = Thrift.Type.INT96;
                            break;
                    }
                } else {
                    tse.Type = Thrift.Type.INT96;
                }
            } else if(st == typeof(TimeSpan)) {         // TimeSpan
                if(field is TimeSpanDataField dfTime) {
                    switch(dfTime.TimeSpanFormat) {
                        case TimeSpanFormat.MilliSeconds:
                            tse.Type = Thrift.Type.INT32;
                            tse.LogicalType.TIME = new Thrift.TimeType {
                                IsAdjustedToUTC = true,
                                Unit = new Thrift.TimeUnit { MILLIS = new Thrift.MilliSeconds() }
                            };
                            tse.Converted_type = Thrift.ConvertedType.TIME_MILLIS;
                            break;
                        case TimeSpanFormat.MicroSeconds:
                            tse.Type = Thrift.Type.INT64;
                            tse.LogicalType.TIME = new Thrift.TimeType {
                                IsAdjustedToUTC = true,
                                Unit = new Thrift.TimeUnit { MICROS = new Thrift.MicroSeconds() }
                            };
                            tse.Converted_type = Thrift.ConvertedType.TIME_MICROS;
                            break;
                        default:
                            throw new NotImplementedException($"{dfTime.TimeSpanFormat} time format is not implemented");
                    }
                } else {
                    tse.Type = Thrift.Type.INT32;
                    tse.LogicalType.TIME = new Thrift.TimeType {
                        IsAdjustedToUTC = true,
                        Unit = new Thrift.TimeUnit { MILLIS = new Thrift.MilliSeconds() }
                    };
                    tse.Converted_type = Thrift.ConvertedType.TIME_MILLIS;
                }
            } else if(st == typeof(Interval)) {         // Interval
                tse.Type = Thrift.Type.FIXED_LEN_BYTE_ARRAY;
                tse.Type_length = 12;
                tse.Converted_type = Thrift.ConvertedType.INTERVAL;
            } else {
                throw new InvalidOperationException($"type {st} is not supported");
            }

            return tse;
        }

        public static void Encode(Field field, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            if(field.SchemaType == SchemaType.Data && field is DataField dataField) {
                Thrift.SchemaElement tse = Encode(dataField);

                bool isList = container.Count > 1 && container[container.Count - 2].Converted_type == Thrift.ConvertedType.LIST;

                tse.Repetition_type = dataField.IsArray && !isList
                   ? Thrift.FieldRepetitionType.REPEATED
                   : (dataField.IsNullable ? Thrift.FieldRepetitionType.OPTIONAL : Thrift.FieldRepetitionType.REQUIRED);
                container.Add(tse);
                parent.Num_children += 1;
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
    }
}