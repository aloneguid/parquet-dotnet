using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
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
        private record struct ReflectedSchemaOptions(bool ForWriting, bool UseNullableAnnotations);
        private static readonly ConcurrentDictionary<(Type, ReflectedSchemaOptions), ParquetSchema> _cachedReflectedSchemas = new();

        /// <summary>
        /// Represents type information associated with a class member, including
        /// nested type information within generics and arrays.
        /// </summary>
        /// <example>
        /// For a property `List[string?] MyList { get; set; }`, there is a ClassMember
        /// representation for the property info + top-level type (list of string?)
        /// and the property info + element type (string?).
        /// </example>
        abstract class ClassMember {
            private readonly MemberInfo _mi;
            private string? _columnName;
            private Type _currentType;

            protected ClassMember(MemberInfo mi, Type currentType) {
                _mi = mi;
                _currentType = currentType;
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

            public Type MemberType => _currentType;

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
            
            /// <summary>
            /// Determines if this member is explicitly nullable based on C# nullable annotations
            /// </summary>
            public bool HasNullableAnnotation()
            {   
#if NET6_0_OR_GREATER
                return _nullabilityInfo.WriteState == NullabilityState.Nullable ||
                        _nullabilityInfo.ReadState == NullabilityState.Nullable;
#else
                return true;
#endif
            }

            protected abstract ClassMember MakeCopy();

            public ClassMember GetClassMemberForEnumerableElement() {
                ClassMember elementCM = MakeCopy();
                if (_currentType.HasElementType) {
                    elementCM._currentType = _currentType.GetElementType()!;
                }
                else {
                    elementCM._currentType = _currentType.GenericTypeArguments[0];
                }

#if NET6_0_OR_GREATER
                if (_nullabilityInfo.ElementType != null)
                {
                    elementCM._nullabilityInfo = _nullabilityInfo.ElementType;
                }
                else
                {
                    elementCM._nullabilityInfo = _nullabilityInfo.GenericTypeArguments[0];
                }
#endif
                return elementCM;
            }

            public (ClassMember, ClassMember) GetClassMemberForDictionaryKVP(Type tKey, Type tValue) {
                Type kvpType = typeof(KeyValuePair<,>).MakeGenericType(tKey, tValue);
                PropertyInfo piKey = kvpType.GetProperty("Key")!;
                PropertyInfo piValue = kvpType.GetProperty("Value")!;
                var cpmKey = new ClassPropertyMember(piKey);
                var cpmValue = new ClassPropertyMember(piValue);
                cpmKey.ColumnName = MapField.KeyName;
                cpmValue.ColumnName = MapField.ValueName;

#if NET6_0_OR_GREATER
                if (_nullabilityInfo.GenericTypeArguments.Length >= 2)
                {
                    cpmKey._nullabilityInfo = _nullabilityInfo.GenericTypeArguments[0];
                    cpmValue._nullabilityInfo = _nullabilityInfo.GenericTypeArguments[1];
                }
#endif
                return (cpmKey, cpmValue);
            }

//             public ClassMember GetClassMemberForGenericTypeArg(int position)
//             {
//                 ClassMember classMember = MakeCopy();
// #if NET6_0_OR_GREATER
//                 classMember._nullabilityInfo = _nullabilityInfo.GenericTypeArguments[position];
// #endif
//                 return classMember;
//             }

//             public ClassMember GetClassMemberForArrayElement()
//             {
//                 ClassMember classMember = MakeCopy();
// #if NET6_0_OR_GREATER
//                 classMember._nullabilityInfo = _nullabilityInfo.ElementType!;
// #endif
//                 return classMember;             
//             }

#if NET6_0_OR_GREATER
            protected NullabilityInfo _nullabilityInfo = null!;
#endif
        }

        class ClassPropertyMember : ClassMember {
            private readonly PropertyInfo _pi;

            public ClassPropertyMember(PropertyInfo propertyInfo) : base(propertyInfo, propertyInfo.PropertyType) {
                _pi = propertyInfo;

#if NET6_0_OR_GREATER
                NullabilityInfoContext context = new NullabilityInfoContext();
                _nullabilityInfo = context.Create(_pi);
#endif
            }

            protected override ClassMember MakeCopy() => new ClassPropertyMember(_pi);
        }

        class ClassFieldMember : ClassMember {
            private readonly FieldInfo _fi;

            public ClassFieldMember(FieldInfo fi) : base(fi, fi.FieldType) {
                _fi = fi;

#if NET6_0_OR_GREATER
                NullabilityInfoContext context = new NullabilityInfoContext();
                _nullabilityInfo = context.Create(_fi);
#endif
            }

            protected override ClassMember MakeCopy() => new ClassFieldMember(_fi);
        }

        /// <summary>
        /// Reflects this type to get <see cref="ParquetSchema"/>
        /// </summary>
        /// <param name="t"></param>
        /// <param name="forWriting">
        /// Set to true to get schema when deserialising into classes (writing to classes), otherwise false.
        /// The result will differ if for instance some properties are read-only and some write-only.
        /// </param>
        /// <param name="useNullableAnnotations">
        /// When true, all reference type fields are required, unless they are explicity marked as
        /// nullable (e.g., with the nullability operator). When false, reference type fields are
        /// non-required unless explicitly marked with the C# required keyword.
        /// </param>
        /// <returns>The parquet schema</returns>
        public static ParquetSchema GetParquetSchema(this Type t, bool forWriting,
         bool useNullableAnnotations = false) {
            ReflectedSchemaOptions options = new ReflectedSchemaOptions(forWriting, useNullableAnnotations);
            if (_cachedReflectedSchemas.TryGetValue((t, options), out ParquetSchema? schema))
            {
                return schema;
            }

            ReflectedSchemaCalculator schemaCalculator = new ReflectedSchemaCalculator(options);
            schema = schemaCalculator.CreateSchema(t);

            _cachedReflectedSchemas[(t, options)] = schema;
            return schema;
        }

        class ReflectedSchemaCalculator(ReflectedSchemaOptions opts) {
            private ReflectedSchemaOptions _opts = opts;   

            private List<ClassMember> FindMembers(Type t) {

                var members = new List<ClassMember>();

                PropertyInfo[] allProps = t.GetProperties(BindingFlags.Instance | BindingFlags.Public);
                members.AddRange((_opts.ForWriting
                    ? allProps.Where(p => p.CanWrite)
                    : allProps.Where(p => p.CanRead)).Select(p => new ClassPropertyMember(p)));

                // fields are always read/write
                members.AddRange(t.GetFields(BindingFlags.Instance | BindingFlags.Public)
                    .Select(f => new ClassFieldMember(f)));

                return members;
            }

            private Field ConstructDataField(string name, string propertyName, Type t, ClassMember member) {
                Field r;

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
                        t = nt.GetEnumUnderlyingType();
                    }
                    if(t.IsEnum) {
                        t = t.GetEnumUnderlyingType();
                    }
                    bool? shouldBeNullable = ShouldClassMemberBeNullable(member);
                    r = new DataField(name, t, shouldBeNullable, null, propertyName);
                }

                return r;
            }

            private bool ShouldClassMemberBeNullable(ClassMember member) {
                if (member.IsRequired) {
                    return false;
                }

                if (Nullable.GetUnderlyingType(member.MemberType) != null) {
                    return true;
                }

                if (!member.MemberType.IsNullable()) {
                    return false;
                }

                if(_opts.UseNullableAnnotations) {
                    return member.HasNullableAnnotation();
                }

                return true;
            }

            private MapField ConstructMapField(string name, string propertyName,
                Type tKey, Type tValue, ClassMember cpmDict) {

                (ClassMember cpmKey, ClassMember cpmValue) = cpmDict.GetClassMemberForDictionaryKVP(tKey, tValue);
                Field keyField = MakeField(cpmKey)!;
                if(keyField is DataField keyDataField && keyDataField.IsNullable) {
                    keyField.IsNullable = false;
                }

                Field valueField = MakeField(cpmValue)!;
                var mf = new MapField(name, keyField, valueField);
                mf.ClrPropName = propertyName;
                mf.IsNullable = ShouldClassMemberBeNullable(cpmDict);
                return mf;
            }

            private ListField ConstructListField(string name, string propertyName,
                Type elementType, ClassMember memberForList) {

                ClassMember memberForElement = memberForList.GetClassMemberForEnumerableElement();
                Field listItemField = MakeField(elementType, ListField.ElementName, propertyName, memberForElement)!;
                if(memberForList.IsListElementRequired) {
                    listItemField.IsNullable = false;
                }

                ListField lf = new ListField(name, listItemField);
                lf.ClrPropName = propertyName;
                lf.IsNullable = ShouldClassMemberBeNullable(memberForList);
                return lf;
            }

            private Field? MakeField(ClassMember member) {
                if(member.ShouldIgnore)
                    return null;

                Field r = MakeField(member.MemberType, member.ColumnName, member.Name, member);
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
            /// <returns><see cref="DataField"/> or complex field (recursively scans class). Can return null if property is explicitly marked to be ignored.</returns>
            /// <exception cref="NotImplementedException"></exception>
            private Field MakeField(Type t, string columnName, string propertyName,
                ClassMember member) {

                Type baseType = t.IsNullable() ? t.GetNonNullable() : t;
                if(member.IsLegacyRepeatable && !baseType.IsGenericIDictionary() && baseType.TryExtractIEnumerableType(out Type? bti)) {
                    baseType = bti!;
                    member = member.GetClassMemberForEnumerableElement();
                }

                if(SchemaEncoder.IsSupported(baseType)) {
                    return ConstructDataField(columnName, propertyName, t, member);
                } else if(t.TryExtractDictionaryType(out Type? tKey, out Type? tValue)) {
                    return ConstructMapField(columnName, propertyName, tKey, tValue, member);
                } else if(t.TryExtractIEnumerableType(out Type? elementType)) {
                    return ConstructListField(columnName, propertyName, elementType!, member);
                } else if(baseType.IsClass || baseType.IsInterface || baseType.IsValueType) {
                    // must be a struct then (c# class, interface or struct)
                    List<ClassMember> props = FindMembers(baseType);
                    Field[] fields = props
                        .Select(p => MakeField(p))
                        .Where(f => f != null)
                        .Select(f => f!)
                        .OrderBy(f => f.Order)
                        .ToArray();

                    if(fields.Length == 0)
                        throw new InvalidOperationException($"property '{propertyName}' has no fields");

                    StructField sf = new StructField(columnName, fields);
                    sf.ClrPropName = propertyName;
                    sf.IsNullable = ShouldClassMemberBeNullable(member);
                    
                    // For struct fields, determine nullability based on the type and member
                    if (member != null) {
                        
                    } else {
                        sf.IsNullable = baseType.IsNullable() || t.IsSystemNullable();
                    }
                    
                    return sf;
                }

                throw new NotImplementedException();
            }

            public ParquetSchema CreateSchema(Type t) {

                // get it all, including base class properties (may be a hierarchy)
                List<ClassMember> props = FindMembers(t);
                List<Field> fields = props
                    .Select(p => MakeField(p))
                    .Where(f => f != null)
                    .Select(f => f!)
                    .OrderBy(f => f.Order)
                    .ToList();

                return new ParquetSchema(fields);
            }
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