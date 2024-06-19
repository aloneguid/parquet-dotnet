using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json.Serialization;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Schema;
using Parquet.Serialization.Attributes;
using Parquet.Utils;

namespace Parquet.Serialization {

    /// <summary>
    /// Makes <see cref="ParquetSchema"/> from type information.
    /// Migrated from SchemaReflector to better fit into C# design strategy.
    /// </summary>
    public static class TypeExtensions {
        private static readonly ConcurrentDictionary<Type, ParquetSchema> _cachedWriteReflectedSchemas = new();
        private static readonly ConcurrentDictionary<Type, ParquetSchema> _cachedReadReflectedSchemas = new();

        abstract class ClassMember {

            private readonly MemberInfo _mi;

            protected ClassMember(MemberInfo mi) {
                _mi = mi;
            }

            public string Name => _mi.Name;

            public string ColumnName {
                get {
                    JsonPropertyNameAttribute? stxt = _mi.GetCustomAttribute<JsonPropertyNameAttribute>();

                    return stxt?.Name ?? _mi.Name;
                }
            }

            public abstract Type MemberType { get; }

            public int? Order {
                get {
#if NETCOREAPP3_1
                    return null;
#else
                    JsonPropertyOrderAttribute? po = _mi.GetCustomAttribute<JsonPropertyOrderAttribute>();
                    return po?.Order;
#endif
                }
            }

            public bool ShouldIgnore {
                get {
                    return _mi.GetCustomAttribute<JsonIgnoreAttribute>() != null || _mi.GetCustomAttribute<ParquetIgnoreAttribute>() != null;
                }
            }

            public bool IsLegacyRepeatable => _mi.GetCustomAttribute<ParquetSimpleRepeatableAttribute>() != null;

            public bool IsRequired => _mi.GetCustomAttribute<ParquetRequiredAttribute>() != null;

            public ParquetTimestampAttribute? TimestampAttribute => _mi.GetCustomAttribute<ParquetTimestampAttribute>();

            public ParquetMicroSecondsTimeAttribute? MicroSecondsTimeAttribute => _mi.GetCustomAttribute<ParquetMicroSecondsTimeAttribute>();

            public ParquetDecimalAttribute? DecimalAttribute => _mi.GetCustomAttribute<ParquetDecimalAttribute>();

            /// <summary>
            /// https://github.com/dotnet/roslyn/blob/main/docs/features/nullable-metadata.md
            /// This check if a class T (nullable by default) doesn't have the nullable mark.
            /// Every class should be considered nullable unless the compiler has been instructed to make it non-nullable.
            /// </summary>
            /// <returns></returns>
            public bool? IsNullable(Type finalType) {
                if(finalType.IsClass == false)
                    return null;
                bool isCompiledWithNullable = _mi.DeclaringType?.CustomAttributes
                        .Any(attr => attr.AttributeType.Name == "NullableAttribute") == true;
                if(!isCompiledWithNullable) {
                    return null;
                }

                // Check if any properties have the NullableContextAttribute
                CustomAttributeData? nullableAttribute = _mi.CustomAttributes
                        .FirstOrDefault(attr => attr.AttributeType.Name == "NullableAttribute");

                byte? attributeFlag = null;
                if(nullableAttribute != null) {
                    if(nullableAttribute.ConstructorArguments[0].Value is byte t) {
                        attributeFlag = t;
                    } else if(nullableAttribute.ConstructorArguments[0].Value is byte[] tArray) {
                        attributeFlag = tArray[0];
                    }
                }
                if(attributeFlag == 1) {
                    return false;
                }
                if(attributeFlag == 2) {
                    return true;
                }

                CustomAttributeData? nullableContextAttribute = _mi.DeclaringType?.CustomAttributes
                        .FirstOrDefault(attr => attr.AttributeType.Name == "NullableContextAttribute");
                byte? classFlag = null;
                if(nullableContextAttribute != null) {
                    classFlag = (byte)nullableContextAttribute.ConstructorArguments[0].Value!;
                }
                if(classFlag == 1) {
                    return false;
                }
                if(classFlag == 2) {
                    return true;
                }

                return null;
            }


        }

        class ClassPropertyMember : ClassMember {
            private readonly PropertyInfo _pi;

            public ClassPropertyMember(PropertyInfo propertyInfo) : base(propertyInfo) {
                _pi = propertyInfo;
            }

            public override Type MemberType => _pi.PropertyType;

        }

        class ClassFieldMember : ClassMember {
            private readonly FieldInfo _fi;

            public ClassFieldMember(FieldInfo fi) : base(fi) {
                _fi = fi;
            }

            public override Type MemberType => _fi.FieldType;
        }

        /// <summary>
        /// Reflects this type to get <see cref="ParquetSchema"/>
        /// </summary>
        /// <param name="t"></param>
        /// <param name="forWriting">
        /// Set to true to get schema when deserialising into classes (writing to classes), otherwise false.
        /// The result will differ if for instance some properties are read-only and some write-only.
        /// </param>
        /// <returns></returns>
        public static ParquetSchema GetParquetSchema(this Type t, bool forWriting) {

            ConcurrentDictionary<Type, ParquetSchema> cache = forWriting
                ? _cachedWriteReflectedSchemas
                : _cachedReadReflectedSchemas;

            if(cache.TryGetValue(t, out ParquetSchema? schema))
                return schema;

            schema = CreateSchema(t, forWriting);

            cache[t] = schema;
            return schema;
        }

        private static List<ClassMember> FindMembers(Type t, bool forWriting) {

            var members = new List<ClassMember>();

            PropertyInfo[] allProps = t.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            members.AddRange((forWriting
                ? allProps.Where(p => p.CanWrite)
                : allProps.Where(p => p.CanRead)).Select(p => new ClassPropertyMember(p)));

            // fields are always read/write
            members.AddRange(t.GetFields(BindingFlags.Instance | BindingFlags.Public)
                .Select(f => new ClassFieldMember(f)));

            return members;
        }

        private static Field ConstructDataField(string name, string propertyName, Type t, ClassMember? member, bool isCompiledWithNullable) {
            Field r;
            bool? isNullable = member == null
                ? null
                : member.IsRequired ? false : null;

            if(t == typeof(DateTime) || t == typeof(DateTime?)) {
                ParquetTimestampAttribute? tsa = member?.TimestampAttribute;
                r = new DateTimeDataField(name,
                    tsa == null ? DateTimeFormat.Impala : tsa.GetDateTimeFormat(),
                    isAdjustedToUTC: tsa == null ? true : tsa.IsAdjustedToUTC,
                    unit: tsa?.Resolution.Convert(),
                    isNullable: t == typeof(DateTime?), null, propertyName);
            } else if (t == typeof(DateTimeOffset) || t == typeof(DateTimeOffset?)) {
                ParquetTimestampAttribute? tsa = member?.TimestampAttribute;
                r = new DateTimeOffsetDataField(name,
                    unit: tsa?.Resolution.Convert(),
                    isNullable: t == typeof(DateTimeOffset?), null, propertyName);
            } else if(t == typeof(TimeSpan) || t == typeof(TimeSpan?)) {
                r = new TimeSpanDataField(name,
                    member?.MicroSecondsTimeAttribute == null
                        ? TimeSpanFormat.MilliSeconds
                        : TimeSpanFormat.MicroSeconds,
                    t == typeof(TimeSpan?), null, propertyName);
#if NET6_0_OR_GREATER
            } else if(t == typeof(TimeOnly) || t == typeof(TimeOnly?)) {
                r = new TimeOnlyDataField(name,
                    member?.MicroSecondsTimeAttribute == null
                        ? TimeSpanFormat.MilliSeconds
                        : TimeSpanFormat.MicroSeconds,
                    t == typeof(TimeOnly?),
                    null, propertyName);
#endif
            } else if(t == typeof(decimal) || t == typeof(decimal?)) {
                ParquetDecimalAttribute? ps = member?.DecimalAttribute;
                bool isTypeNullable = t == typeof(decimal?);
                r = ps == null
                    ? new DecimalDataField(name,
                        DecimalFormatDefaults.DefaultPrecision, DecimalFormatDefaults.DefaultScale,
                            isNullable: isTypeNullable, propertyName: propertyName)
                    : new DecimalDataField(name, ps.Precision, ps.Scale,
                        isNullable: isTypeNullable, propertyName: propertyName);
            } else {

                if(t.IsEnum) {
                    t = t.GetEnumUnderlyingType();
                }
                bool? isMemberNullable = null;
                if (isCompiledWithNullable) {
                    isMemberNullable = member?.IsNullable(t);
                }
                
                if(isMemberNullable is not null) {
                    isNullable = isMemberNullable.Value;
                }
                r = new DataField(name, t, isNullable, null, propertyName, isCompiledWithNullable);
            }

            return r;
        }

        private static MapField ConstructMapField(string name, string propertyName,
            Type tKey, Type tValue,
            bool forWriting,
            bool isCompiledWithNullable) {

            Type kvpType = typeof(KeyValuePair<,>).MakeGenericType(tKey, tValue);
            PropertyInfo piKey = kvpType.GetProperty("Key")!;
            PropertyInfo piValue = kvpType.GetProperty("Value")!;

            Field keyField = MakeField(new ClassPropertyMember(piKey), forWriting, isCompiledWithNullable)!;
            if(keyField is DataField keyDataField && keyDataField.IsNullable) {
                keyField.IsNullable = false;
            }
            Field valueField = MakeField(new ClassPropertyMember(piValue), forWriting, isCompiledWithNullable)!;
            var mf = new MapField(name, keyField, valueField);
            mf.ClrPropName = propertyName;
            return mf;
        }

        private static ListField ConstructListField(string name, string propertyName,
            Type elementType,
            bool forWriting,
            bool isCompiledWithNullable) {

            ListField lf = new ListField(name, MakeField(elementType, ListField.ElementName, propertyName, null, forWriting, isCompiledWithNullable)!);
            lf.ClrPropName = propertyName;
            return lf;
        }

        private static Field? MakeField(ClassMember member, bool forWriting, bool isCompiledWithNullable) {
            if(member.ShouldIgnore)
                return null;

            Field r = MakeField(member.MemberType, member.ColumnName, member.Name, member, forWriting, isCompiledWithNullable);
            r.Order = member.Order;
            return r;
        }

        /// <summary>
        /// Makes field from property. 
        /// </summary>
        /// <param name="t">Type of property</param>
        /// <param name="columnName">Parquet file column name</param>
        /// <param name="propertyName">Class property name</param>
        /// <param name="member">Optional <see cref="PropertyInfo"/> that can be used to get attribute metadata.</param>
        /// <param name="forWriting"></param>
        /// <param name="isCompiledWithNullable">if nullable was enabled to compile the type</param>
        /// <returns><see cref="DataField"/> or complex field (recursively scans class). Can return null if property is explicitly marked to be ignored.</returns>
        /// <exception cref="NotImplementedException"></exception>
        private static Field MakeField(Type t, string columnName, string propertyName,
            ClassMember? member,
            bool forWriting,
            bool isCompiledWithNullable) {

            Type bt = t.IsNullable() ? t.GetNonNullable() : t;
            if(member != null && member.IsLegacyRepeatable && !bt.IsGenericIDictionary() && bt.TryExtractIEnumerableType(out Type? bti)) {
                bt = bti!;
            }

            if(SchemaEncoder.IsSupported(bt)) {
                return ConstructDataField(columnName, propertyName, t, member, isCompiledWithNullable && !(member?.IsLegacyRepeatable??false));
            } else if(t.TryExtractDictionaryType(out Type? tKey, out Type? tValue)) {
                return ConstructMapField(columnName, propertyName, tKey!, tValue!, forWriting, isCompiledWithNullable);
            } else if(t.TryExtractIEnumerableType(out Type? elementType)) {
                return ConstructListField(columnName, propertyName, elementType!, forWriting, isCompiledWithNullable);
            } else if(t.IsClass || t.IsInterface || t.IsValueType) {
                // must be a struct then (c# class or c# struct)!
                List<ClassMember> props = FindMembers(t, forWriting);
                Field[] fields = props
                    .Select(p => MakeField(p, forWriting, isCompiledWithNullable))
                    .Where(f => f != null)
                    .Select(f => f!)
                    .OrderBy(f => f.Order)
                    .ToArray();

                if(fields.Length == 0)
                    throw new InvalidOperationException($"property '{propertyName}' has no fields");

                StructField sf = new StructField(columnName, fields);
                sf.ClrPropName = propertyName;
                return sf;
            }

            throw new NotImplementedException();
        }

        private static ParquetSchema CreateSchema(Type t, bool forWriting) {

            // get it all, including base class properties (may be a hierarchy)
            bool isCompiledWithNullable = NullableChecker.IsNullableEnabled(t);
            List<ClassMember> props = FindMembers(t, forWriting);
            List<Field> fields = props
                .Select(p => MakeField(p, forWriting, isCompiledWithNullable))
                .Where(f => f != null)
                .Select(f => f!)
                .OrderBy(f => f.Order)
                .ToList();

            return new ParquetSchema(fields);
        }
        
        /// <summary>
        /// Convert Resolution to TimeUnit
        /// </summary>
        /// <param name="resolution"></param>
        /// <returns></returns>
        public static DateTimeTimeUnit Convert(this ParquetTimestampResolution resolution) {
            switch(resolution) {
                case ParquetTimestampResolution.Milliseconds:
                    return DateTimeTimeUnit.Millis;
#if NET7_0_OR_GREATER
                case ParquetTimestampResolution.Microseconds:
                    return DateTimeTimeUnit.Micros;
#endif
                default:
                    throw new ParquetException($"Unexpected Resolution: {resolution}");
                // nanoseconds to be added
            }
        }
        
        /// <summary>
        /// Convert Parquet TimeUnit to TimeUnit
        /// </summary>
        /// <param name="unit"></param>
        /// <returns></returns>
        public static DateTimeTimeUnit Convert(this Parquet.Meta.TimeUnit unit) {
            if(unit.MILLIS is not null) {
                return DateTimeTimeUnit.Millis;
            }

            if(unit.MICROS is not null) {
                return DateTimeTimeUnit.Micros;
            }

            if(unit.NANOS is not null) {
                return DateTimeTimeUnit.Nanos;
            }

            throw new ParquetException($"Unexpected TimeUnit: {unit}");
        }
    }
}