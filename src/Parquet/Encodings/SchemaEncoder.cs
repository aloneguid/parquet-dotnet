using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using Parquet.Data;
using Parquet.File.Values.Primitives;
using Parquet.Meta;
using Parquet.Schema;
using Parquet.Serialization;
using SType = System.Type;
using Type = Parquet.Meta.Type;

namespace Parquet.Encodings;

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
        typeof(BigDecimal),
        typeof(BigInteger),
        typeof(DateTime),
        typeof(DateOnly),
        typeof(TimeOnly),
        typeof(TimeSpan),
        typeof(Interval),
        typeof(byte[]),
        typeof(string),
        typeof(Guid)
    };

    static bool TryBuildList(List<SchemaElement> schema,
        ref int index, out int ownedChildren,
        out ListField? field) {

        // exception:
        // A repeated field that is neither contained by a LIST- or MAP-annotated group nor annotated by LIST or MAP should be interpreted as a required list of required elements where the element type is the type of the field.
        // It's called "implicit list" in this codebase.

        SchemaElement? parent = index > 0 ? schema[index - 1] : null;
        SchemaElement outerGroup = schema[index];

        bool isList = outerGroup.IsList();
        // According to the Parquet spec, a repeated field is an implicit list only if it is not contained by a LIST- or MAP-annotated group,
        // nor annotated by LIST or MAP itself.
        bool isImplicitList = (parent == null || parent.ConvertedType == null) &&
            outerGroup.RepetitionType == FieldRepetitionType.REPEATED &&
            !outerGroup.IsMap();

        if(!isList && !isImplicitList) {
            ownedChildren = 0;
            field = null;
            return false;
        }

        bool isNullable = !isImplicitList && outerGroup.RepetitionType != FieldRepetitionType.REQUIRED;
        field = ListField.CreateWithNoItem(outerGroup.Name, isNullable);

        if(isImplicitList) {
            if(outerGroup.NumChildren <= 0) {
                throw new IndexOutOfRangeException("Implicit list must have at least one child element");
            }

            field.SchemaElement = outerGroup;
            field.GroupSchemaElement = outerGroup;  // for implicit
            field.Path = new FieldPath(outerGroup.Name);
            if(outerGroup.NumChildren == 1) {
                index += 1;
                ownedChildren = 1; //next element is the list's item
                return true;
            } else {
                throw new NotSupportedException("Implicit list with multiple children is currently not supported");
            }
        }

        //https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
        SchemaElement midGroup = schema[index + 1];
        bool midIsGroup = midGroup.NumChildren > 0;

        // Rule 1. If the repeated field is not a group, then its type is the element type and elements are required.
        if(!midIsGroup) {
            field.Path = new FieldPath(outerGroup.Name);
            field.SchemaElement = outerGroup;
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
        field.SchemaElement = outerGroup;
        field.GroupSchemaElement = midGroup;
        index += 2;          //skip this element and child container
        ownedChildren = 1;   //we should get this element assigned back
        return true;
    }

    static bool TryBuildMap(List<SchemaElement> schema,
        ref int index, out int ownedChildren,
        out MapField? field) {

        SchemaElement root = schema[index];
        if(!root.IsMap()) {
            ownedChildren = 0;
            field = null;
            return false;
        }

        //next element is a container
        SchemaElement tseContainer = schema[++index];

        if(tseContainer.NumChildren != 2) {
            throw new IndexOutOfRangeException($"dictionary container must have exactly 2 children but {tseContainer.NumChildren} found");
        }

        //followed by a key and a value, but we declared them as owned

        var map = new MapField(root.Name) {
            Path = new FieldPath(root.Name, tseContainer.Name),
            IsNullable = root.RepetitionType != FieldRepetitionType.REQUIRED,
            GroupSchemaElement = tseContainer,
            SchemaElement = root
        };

        index += 1;
        ownedChildren = 2;
        field = map;
        return true;
    }

    static bool TryBuildStruct(List<SchemaElement> schema,
        ref int index, out int ownedChildren,
        out StructField? field) {
        SchemaElement container = schema[index];
        bool isStruct = container.NumChildren > 0;
        if(!isStruct) {
            ownedChildren = 0;
            field = null;
            return false;
        }

        index++;
        ownedChildren = container.NumChildren ?? 0; //make them owned to receive in .Assign()
        field = StructField.CreateWithNoElements(container.Name);
        field.IsNullable = container.RepetitionType != FieldRepetitionType.REQUIRED;
        field.SchemaElement = container;
        return true;
    }

    public static bool IsSupported(SType t) => t.IsEnum || SupportedTypes.Contains(t);

    /// <summary>
    /// Builds <see cref="Field"/> from schema
    /// </summary>
    /// <param name="schema"></param>
    /// <param name="options"></param>
    /// <param name="index"></param>
    /// <param name="ownedChildCount"></param>
    /// <returns></returns>
    public static Field? Decode(List<SchemaElement> schema,
        ParquetOptions options,
        ref int index, out int ownedChildCount) {

        SchemaElement se = schema[index];
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

    /// <summary>
    /// See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
    /// </summary>
    private static SType? GetSystemType(SchemaElement se, ParquetOptions options) {

        // not set for non-leaf nodes (map, list etc.)
        if(se.Type == null)
            return null;

        // try to get the type base on annotation first

        // ---- strings

        if(se.LogicalType?.STRING != null ||
            se.LogicalType?.ENUM != null ||
            se.ConvertedType == ConvertedType.UTF8) {
            return typeof(string);
        }

        // --- numeric types

        if(se.LogicalType?.INTEGER != null) {
            IntType intType = se.LogicalType.INTEGER;
            switch(intType.BitWidth) {
                case 8:
                    return intType.IsSigned ? typeof(sbyte) : typeof(byte);
                case 16:
                    return intType.IsSigned ? typeof(short) : typeof(ushort);
                case 32:
                    return intType.IsSigned ? typeof(int) : typeof(uint);
                case 64:
                    return intType.IsSigned ? typeof(long) : typeof(ulong);
            }
        }

        // legacy types
        if(se.ConvertedType == ConvertedType.INT_8)
            return typeof(sbyte);
        else if(se.ConvertedType == ConvertedType.UINT_8)
            return typeof(byte);
        else if(se.ConvertedType == ConvertedType.INT_16)
            return typeof(short);
        else if(se.ConvertedType == ConvertedType.UINT_16)
            return typeof(ushort);
        else if(se.ConvertedType == ConvertedType.INT_32)
            return typeof(int);
        else if(se.ConvertedType == ConvertedType.UINT_32)
            return typeof(uint);
        else if(se.ConvertedType == ConvertedType.INT_64)
            return typeof(long);
        else if(se.ConvertedType == ConvertedType.UINT_64)
            return typeof(ulong);

        // --- decimal types

        if(se.LogicalType?.DECIMAL != null || se.ConvertedType == ConvertedType.DECIMAL) {
            return options.DecimalType;
        }

        // --- float16

        if(se.LogicalType?.FLOAT16 != null) {
            return typeof(System.Half);
        }

        // --- temporal types

        // date
        if(se.LogicalType?.DATE != null || se.ConvertedType == ConvertedType.DATE) {
            return options.UseDateOnlyTypeForDates ? typeof(DateOnly) : typeof(DateTime);
        }

        // time
        if(se.LogicalType?.TIME != null) {
            TimeType timeType = se.LogicalType.TIME;
            if(timeType.Unit.MILLIS != null ) {
                return options.UseTimeOnlyTypeForTimeMillis ? typeof(TimeOnly) : typeof(TimeSpan);
            }
            if(timeType.Unit.MICROS != null) {
                return options.UseTimeOnlyTypeForTimeMicros ? typeof(TimeOnly) :  typeof(TimeSpan);
            }
            if(timeType.Unit.NANOS != null) {
                return typeof(DateTime);
            }
        }

        if(se.ConvertedType == ConvertedType.TIME_MICROS) {
            return options.UseTimeOnlyTypeForTimeMicros ? typeof(TimeOnly) : typeof(TimeSpan);
        }
        if(se.ConvertedType == ConvertedType.TIMESTAMP_MICROS) {
            return typeof(DateTime);
        }
        if(se.ConvertedType == ConvertedType.TIMESTAMP_MILLIS) {
            return typeof(DateTime);
        }

        // timestamp
        if(se.LogicalType?.TIMESTAMP != null) {
            return typeof(DateTime);
        }
        if(se.ConvertedType == ConvertedType.INTERVAL) {
            return typeof(Interval);
        }

        // if we are still here, try physical types
        switch(se.Type) {
            case Type.BOOLEAN:
                return typeof(bool);
            case Type.INT32:
                return typeof(int);
            case Type.INT64:
                return typeof(long);
            case Type.INT96:
                return options.TreatBigIntegersAsDates ? typeof(DateTime) : typeof(BigInteger);
            case Type.FLOAT:
                return typeof(float);
            case Type.DOUBLE:
                return typeof(double);
            case Type.BYTE_ARRAY:
                return options.TreatByteArrayAsString ? typeof(string) : typeof(byte[]);
            case Type.FIXED_LEN_BYTE_ARRAY:
                if(se.LogicalType?.UUID != null)
                    return typeof(Guid);
                if(se.LogicalType?.DECIMAL != null || se.ConvertedType == ConvertedType.DECIMAL)
                    return options.DecimalType;
                return typeof(byte[]);
        }

        return null;
    }

    private static bool TryBuildDataField(SchemaElement se, ParquetOptions options, out DataField? df) {
        df = null;

        if(se.Type == null)
            return false;

        SType? systemType = GetSystemType(se, options);
        if(systemType == null)
            throw new NotSupportedException($"unsupported type {se.Type}");

        if(systemType == typeof(DateTime)) {
            df = GetDateTimeDataField(se);
        } else if(systemType == options.DecimalType) {
            df = GetDecimalDataField(se, options.DecimalType);
        } else {
            // successful field built
            df = new DataField(se.Name, systemType);
        }
        bool isNullable = se.RepetitionType != FieldRepetitionType.REQUIRED;
        bool isArray = se.RepetitionType == FieldRepetitionType.REPEATED;
        df.IsNullable = isNullable;
        df.IsArray = isArray;
        df.SchemaElement = se;
        return true;
    }

    private static DataField GetDecimalDataField(SchemaElement se, SType clrType) =>
        new DecimalDataField(se.Name,
            se.Precision.GetValueOrDefault(DecimalFormatDefaults.DefaultPrecision),
            se.Scale.GetValueOrDefault(se.Precision is not null ? 0 : DecimalFormatDefaults.DefaultScale),
            clrType: clrType);

    private static DataField GetDateTimeDataField(SchemaElement se) {
        if(se.LogicalType is not null)
            if(se.LogicalType.TIMESTAMP is not null)
                return new DateTimeDataField(se.Name, DateTimeFormat.Timestamp, isAdjustedToUTC: se.LogicalType.TIMESTAMP.IsAdjustedToUTC, unit: se.LogicalType.TIMESTAMP.Unit.Convert());

        if(se.IsTimestampMillis())
            return new DateTimeDataField(se.Name, DateTimeFormat.DateAndTime);

        if(se.IsTimestampMicros())
            return new DateTimeDataField(se.Name, DateTimeFormat.DateAndTimeMicros);

        if(se.IsDate())
            return new DateTimeDataField(se.Name, DateTimeFormat.Date);

        return new DateTimeDataField(se.Name, DateTimeFormat.Impala);
    }

    private static void Encode(ListField listField, SchemaElement parent, IList<SchemaElement> container, ParquetOptions options) {
        parent.NumChildren = (parent.NumChildren ?? 0) + 1;

        //add list container
        var root = new SchemaElement {
            Name = listField.Name,
            ConvertedType = ConvertedType.LIST,
            LogicalType = new LogicalType { LIST = new ListType() },
            RepetitionType = listField.IsNullable ? FieldRepetitionType.OPTIONAL : FieldRepetitionType.REQUIRED,
            NumChildren = 1  //field container below
        };
        container.Add(root);

        //add field container
        var list = new SchemaElement {
            Name = listField.ContainerName,
            RepetitionType = FieldRepetitionType.REPEATED
        };
        container.Add(list);

        //add the list item as well
        Encode(listField.Item, list, container, options);
    }

    private static void Encode(MapField mapField, SchemaElement parent, IList<SchemaElement> container, ParquetOptions options) {
        parent.NumChildren = (parent.NumChildren ?? 0) + 1;

        //add the root container where map begins
        var root = new SchemaElement {
            Name = mapField.Name,
            ConvertedType = ConvertedType.MAP,
            LogicalType = new LogicalType { MAP = new MapType() },
            NumChildren = 1,
            RepetitionType = FieldRepetitionType.OPTIONAL
        };
        container.Add(root);

        //key-value is a container for column of keys and column of values
        var keyValue = new SchemaElement {
            Name = MapField.ContainerName,
            NumChildren = 0, //is assigned by children
            RepetitionType = FieldRepetitionType.REPEATED
        };
        container.Add(keyValue);

        //now add the key and value separately
        Encode(mapField.Key, keyValue, container, options);
        SchemaElement tseKey = container[container.Count - 1];
        Encode(mapField.Value, keyValue, container, options);
        SchemaElement tseValue = container[container.Count - 1];

        //fixes for weirdness in RLs
        if(tseKey.RepetitionType == FieldRepetitionType.REPEATED)
            tseKey.RepetitionType = FieldRepetitionType.OPTIONAL;
        if(tseValue.RepetitionType == FieldRepetitionType.REPEATED)
            tseValue.RepetitionType = FieldRepetitionType.OPTIONAL;
    }

    private static void Encode(StructField structField, SchemaElement parent, IList<SchemaElement> container, ParquetOptions options) {
        var tseStruct = new SchemaElement {
            Name = structField.Name,
            RepetitionType = structField.IsNullable ? FieldRepetitionType.OPTIONAL : FieldRepetitionType.REQUIRED
            // no logical or converted type annotations for structs
        };
        container.Add(tseStruct);
        parent.NumChildren = (parent.NumChildren ?? 0) + 1;

        foreach(Field memberField in structField.Fields) {
            Encode(memberField, tseStruct, container, options);
        }
    }

    public static SchemaElement Encode(DataField field, ParquetOptions options) {
        SType st = field.ClrType;
        var tse = new SchemaElement { Name = field.Name };

        if( field.FieldId != -1 ) { 
            tse.FieldId = field.FieldId;
        }

        if(st == typeof(bool)) {                                // boolean
            tse.Type = Type.BOOLEAN;
        } else if(st == typeof(byte) || st == typeof(sbyte) ||  // 32-bit numbers
            st == typeof(short) || st == typeof(ushort) ||
            st == typeof(int) || st == typeof(uint)) {

            tse.Type = Type.INT32;
            sbyte bw = 0;
            if(st == typeof(byte) || st == typeof(sbyte))
                bw = 8;
            else if(st == typeof(short) || st == typeof(ushort))
                bw = 16;
            else if(st == typeof(int) || st == typeof(uint))
                bw = 32;
            bool signed = st == typeof(sbyte) || st == typeof(short) || st == typeof(int);

            tse.LogicalType = new LogicalType {
                INTEGER = new IntType {
                    BitWidth = bw,
                    IsSigned = signed
                }
            };
            tse.ConvertedType = bw switch {
                8 => signed ? ConvertedType.INT_8 : ConvertedType.UINT_8,
                16 => signed ? ConvertedType.INT_16 : ConvertedType.UINT_16,
                32 => signed ? ConvertedType.INT_32 : ConvertedType.UINT_32,
                _ => ConvertedType.INT_32
            };
        } else if(st == typeof(long) || st == typeof(ulong)) {  // 64-bit number
            tse.Type = Type.INT64;
            tse.LogicalType = new LogicalType {
                INTEGER = new IntType {
                    BitWidth = 64,
                    IsSigned = st == typeof(long)
                }
            };
            tse.ConvertedType = st == typeof(long) ? ConvertedType.INT_64 : ConvertedType.UINT_64;
        } else if(st == typeof(float)) {                        // float
            tse.Type = Type.FLOAT;
        } else if(st == typeof(double)) {                       // double
            tse.Type = Type.DOUBLE;
        } else if(st == typeof(BigInteger)) {                   // BigInteger
            tse.Type = Type.INT96;
        } else if(st == typeof(string)) {                       // string
            tse.Type = Type.BYTE_ARRAY;
            tse.LogicalType = new LogicalType {
                STRING = new StringType()
            };
            tse.ConvertedType = ConvertedType.UTF8;
        } else if(st == typeof(decimal) || st == typeof(BigDecimal)) {                      // decimal

            int precision;
            int scale;

            if(field is DecimalDataField dfDecimal) {
                if(dfDecimal.ForceByteArrayEncoding)
                    tse.Type = Type.FIXED_LEN_BYTE_ARRAY;
                else if(dfDecimal.Precision <= 9)
                    tse.Type = Type.INT32;
                else if(dfDecimal.Precision <= 18)
                    tse.Type = Type.INT64;
                else
                    tse.Type = Type.FIXED_LEN_BYTE_ARRAY;

                precision = dfDecimal.Precision;
                scale = dfDecimal.Scale;
                tse.TypeLength = BigDecimal.GetBufferSize(dfDecimal.Precision);
            } else {
                //set defaults
                tse.Type = Type.FIXED_LEN_BYTE_ARRAY;
                precision = DecimalFormatDefaults.DefaultPrecision;
                scale = DecimalFormatDefaults.DefaultScale;
                tse.TypeLength = 16;
            }

            tse.LogicalType = new LogicalType {
                DECIMAL = new DecimalType {
                    Precision = precision,
                    Scale = scale
                }
            };
            tse.ConvertedType = ConvertedType.DECIMAL;
            tse.Precision = precision;
            tse.Scale = scale;
        } else if(st == typeof(byte[])) {           // byte[]
            tse.Type = Type.BYTE_ARRAY;
        } else if(st == typeof(DateTime)) {         // DateTime
            if(field is DateTimeDataField dfDateTime) {
                switch(dfDateTime.DateTimeFormat) {
                    case DateTimeFormat.DateAndTime:
                        tse.Type = Type.INT64;
                        tse.ConvertedType = ConvertedType.TIMESTAMP_MILLIS;
                        break;
                    case DateTimeFormat.DateAndTimeMicros:
                        tse.Type = Type.INT64;
                        tse.ConvertedType = ConvertedType.TIMESTAMP_MICROS;
                        break;
                    case DateTimeFormat.Date:
                        tse.Type = Type.INT32;
                        tse.ConvertedType = ConvertedType.DATE;
                        break;
                    case DateTimeFormat.Timestamp:
                        tse.Type = Type.INT64;
                        tse.LogicalType = new LogicalType { TIMESTAMP = new TimestampType {
                                IsAdjustedToUTC = dfDateTime.IsAdjustedToUTC,
                                Unit = dfDateTime.Unit switch {
                                    DateTimeTimeUnit.Millis => new TimeUnit {
                                        MILLIS = new MilliSeconds(),
                                    },
                                    DateTimeTimeUnit.Micros => new TimeUnit {
                                        MICROS = new MicroSeconds(),
                                    },
                                    DateTimeTimeUnit.Nanos => new TimeUnit {
                                        NANOS = new NanoSeconds(),
                                    },
                                    _ => throw new ParquetException($"Unexpected TimeUnit: {dfDateTime.Unit}")
                                }
                            }
                        };
                        break;
                    default:
                        tse.Type = Type.INT96;
                        break;
                }
            } else {
                tse.Type = Type.INT96;
            }
#if NET6_0_OR_GREATER
        } else if(st == typeof(DateOnly)) {
            // DateOnly
            tse.Type = Type.INT32;
            tse.LogicalType = new LogicalType { DATE = new DateType() };
            tse.ConvertedType = ConvertedType.DATE;
        } else if (st == typeof(TimeOnly)) {
            // TimeOnly
            if(field is TimeOnlyDataField dfTime) {
                switch(dfTime.TimeSpanFormat) {
                    case TimeSpanFormat.MilliSeconds:
                        tse.Type = Type.INT32;
                        tse.LogicalType = new LogicalType {
                            TIME = new TimeType {
                                IsAdjustedToUTC = dfTime.IsAdjustedToUTC,
                                Unit = new TimeUnit { MILLIS = new MilliSeconds() }
                            }
                        };
                        tse.ConvertedType = ConvertedType.TIME_MILLIS;
                        break;
                    case TimeSpanFormat.MicroSeconds:
                        tse.Type = Type.INT64;
                        tse.LogicalType = new LogicalType {
                            TIME = new TimeType {
                                IsAdjustedToUTC = dfTime.IsAdjustedToUTC,
                                Unit = new TimeUnit { MICROS = new MicroSeconds() }
                            }
                        };
                        tse.ConvertedType = ConvertedType.TIME_MICROS;
                        break;
                    default:
                        throw new NotImplementedException($"{dfTime.TimeSpanFormat} time format is not implemented");
                }
            } else {
                tse.Type = Type.INT32;
                tse.LogicalType = new LogicalType {
                    TIME = new TimeType() {
                        IsAdjustedToUTC = true,
                        Unit = new TimeUnit { MILLIS = new MilliSeconds() }
                    }
                };
                tse.ConvertedType = ConvertedType.TIME_MILLIS;
            }
#endif
        } else if(st == typeof(TimeSpan)) {         // TimeSpan
            if(field is TimeSpanDataField dfTime) {
                switch(dfTime.TimeSpanFormat) {
                    case TimeSpanFormat.MilliSeconds:
                        tse.Type = Type.INT32;
                        tse.LogicalType = new LogicalType {
                            TIME = new TimeType {
                                IsAdjustedToUTC = dfTime.IsAdjustedToUTC,
                                Unit = new TimeUnit { MILLIS = new MilliSeconds() }
                            }
                        };
                        tse.ConvertedType = ConvertedType.TIME_MILLIS;
                        break;
                    case TimeSpanFormat.MicroSeconds:
                        tse.Type = Type.INT64;
                        tse.LogicalType = new LogicalType {
                            TIME = new TimeType {
                                IsAdjustedToUTC = dfTime.IsAdjustedToUTC,
                                Unit = new TimeUnit { MICROS = new MicroSeconds() }
                            }
                        };
                        tse.ConvertedType = ConvertedType.TIME_MICROS;
                        break;
                    default:
                        throw new NotImplementedException($"{dfTime.TimeSpanFormat} time format is not implemented");
                }
            } else {
                tse.Type = Type.INT32;
                tse.LogicalType = new LogicalType {
                    TIME = new TimeType {
                        IsAdjustedToUTC = true,
                        Unit = new TimeUnit { MILLIS = new MilliSeconds() }
                    }
                };
                tse.ConvertedType = ConvertedType.TIME_MILLIS;
            }
        } else if(st == typeof(Interval)) {         // Interval
            tse.Type = Type.FIXED_LEN_BYTE_ARRAY;
            tse.TypeLength = 12;
            tse.ConvertedType = ConvertedType.INTERVAL;
        } else if(st == typeof(Guid)) {
            // fixed_len_byte_array(16) uuid (UUID)
            tse.Type = Type.FIXED_LEN_BYTE_ARRAY;
            tse.LogicalType = new LogicalType() {
                UUID = new UUIDType()
            };
            tse.TypeLength = 16;
        } else {
            throw new InvalidOperationException($"type {st} is not supported");
        }

        return tse;
    }

    public static void Encode(Field field, SchemaElement parent, IList<SchemaElement> container, ParquetOptions options) {
        if(field.SchemaType == SchemaType.Data && field is DataField dataField) {
            SchemaElement tse = Encode(dataField, options);

            bool isList = container.Count > 1 && container[container.Count - 2].IsList();

            tse.RepetitionType = dataField.IsArray && !isList
               ? FieldRepetitionType.REPEATED
               : (dataField.IsNullable ? FieldRepetitionType.OPTIONAL : FieldRepetitionType.REQUIRED);
            container.Add(tse);
            parent.NumChildren = (parent.NumChildren ?? 0) + 1;
        } else if(field.SchemaType == SchemaType.List && field is ListField listField) {
            Encode(listField, parent, container, options);
        } else if(field.SchemaType == SchemaType.Map && field is MapField mapField) {
            Encode(mapField, parent, container, options);
        } else if(field.SchemaType == SchemaType.Struct && field is StructField structField) {
            Encode(structField, parent, container, options);
        } else {
            throw new InvalidOperationException($"unable to encode {field}");
        }
    }
}