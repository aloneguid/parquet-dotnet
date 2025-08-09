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
            private string? _columnName;

            protected ClassMember(MemberInfo mi) {
                _mi = mi;
            }

            public string Name => _mi.Name;

            /// <summary>
            /// Parquet column name for this member. Will check appropriate attribute to detect name.
            /// </summary>
            public string ColumnName {
                get {
                    if(_columnName == null) {
                        JsonPropertyNameAttribute? stxt = _mi.GetCustomAttribute<JsonPropertyNameAttribute>();
                        _columnName = stxt?.Name ?? _mi.Name;
                    }

                    return _columnName;
                }
                internal set => _columnName = value;
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

            public bool IsListElementRequired => _mi.GetCustomAttribute<ParquetListElementRequiredAttribute>() != null;

            public ParquetTimestampAttribute? TimestampAttribute => _mi.GetCustomAttribute<ParquetTimestampAttribute>();

            public ParquetMicroSecondsTimeAttribute? MicroSecondsTimeAttribute => _mi.GetCustomAttribute<ParquetMicroSecondsTimeAttribute>();

            public ParquetDecimalAttribute? DecimalAttribute => _mi.GetCustomAttribute<ParquetDecimalAttribute>();
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

        private static Field ConstructDataField(string name, string propertyName, Type t, ClassMember? member) {
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
                Type? nt = Nullable.GetUnderlyingType(t);
                if(nt is { IsEnum: true }) {
                    isNullable = true;
                    t = nt.GetEnumUnderlyingType();
                }
                if(t.IsEnum) {
                    t = t.GetEnumUnderlyingType();
                }

                r = new DataField(name, t, isNullable, null, propertyName);
            }

            return r;
        }

        private static MapField ConstructMapField(string name, string propertyName,
            Type tKey, Type tValue,
            bool forWriting) {

            Type kvpType = typeof(KeyValuePair<,>).MakeGenericType(tKey, tValue);
            PropertyInfo piKey = kvpType.GetProperty("Key")!;
            PropertyInfo piValue = kvpType.GetProperty("Value")!;
            var cpmKey = new ClassPropertyMember(piKey);
            var cpmValue = new ClassPropertyMember(piValue);
            cpmKey.ColumnName = MapField.KeyName;
            cpmValue.ColumnName = MapField.ValueName;

            Field keyField = MakeField(cpmKey, forWriting)!;
            if(keyField is DataField keyDataField && keyDataField.IsNullable) {
                keyField.IsNullable = false;
            }
            Field valueField = MakeField(cpmValue, forWriting)!;
            var mf = new MapField(name, keyField, valueField);
            mf.ClrPropName = propertyName;
            return mf;
        }

        private static ListField ConstructListField(string name, string propertyName,
            Type elementType,
            ClassMember? member,
            bool forWriting) {

            Field listItemField = MakeField(elementType, ListField.ElementName, propertyName, member, forWriting)!;
            if(member != null && member.IsListElementRequired) {
                listItemField.IsNullable = false;
            }
            ListField lf = new ListField(name, listItemField);
            lf.ClrPropName = propertyName;
            if(member != null && member.IsRequired) {
                lf.IsNullable = false;
            }
            return lf;
        }

        private static Field? MakeField(ClassMember member, bool forWriting) {
            if(member.ShouldIgnore)
                return null;

            Field r = MakeField(member.MemberType, member.ColumnName, member.Name, member, forWriting);
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
        /// <returns><see cref="DataField"/> or complex field (recursively scans class). Can return null if property is explicitly marked to be ignored.</returns>
        /// <exception cref="NotImplementedException"></exception>
        private static Field MakeField(Type t, string columnName, string propertyName,
            ClassMember? member,
            bool forWriting) {

            Type baseType = t.IsNullable() ? t.GetNonNullable() : t;
            if(member != null && member.IsLegacyRepeatable && !baseType.IsGenericIDictionary() && baseType.TryExtractIEnumerableType(out Type? bti)) {
                baseType = bti!;
            }

            if(SchemaEncoder.IsSupported(baseType)) {
                return ConstructDataField(columnName, propertyName, t, member);
            } else if(t.TryExtractDictionaryType(out Type? tKey, out Type? tValue)) {
                return ConstructMapField(columnName, propertyName, tKey!, tValue!, forWriting);
            } else if(t.TryExtractIEnumerableType(out Type? elementType)) {
                return ConstructListField(columnName, propertyName, elementType!, member, forWriting);
            } else if(baseType.IsClass || baseType.IsInterface || baseType.IsValueType) {
                // must be a struct then (c# class, interface or struct)
                List<ClassMember> props = FindMembers(baseType, forWriting);
                Field[] fields = props
                    .Select(p => MakeField(p, forWriting))
                    .Where(f => f != null)
                    .Select(f => f!)
                    .OrderBy(f => f.Order)
                    .ToArray();

                if(fields.Length == 0)
                    throw new InvalidOperationException($"property '{propertyName}' has no fields");

                StructField sf = new StructField(columnName, fields);
                sf.ClrPropName = propertyName;
                sf.IsNullable = baseType.IsNullable() || t.IsSystemNullable();
                return sf;
            }

            throw new NotImplementedException();
        }

        private static ParquetSchema CreateSchema(Type t, bool forWriting) {

            // get it all, including base class properties (may be a hierarchy)
            List<ClassMember> props = FindMembers(t, forWriting);
            List<Field> fields = props
                .Select(p => MakeField(p, forWriting))
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