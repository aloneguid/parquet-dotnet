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

namespace Parquet.Serialization {

    /// <summary>
    /// Makes <see cref="ParquetSchema"/> from type information.
    /// Migrated from SchemaReflector to better fit into C# design strategy.
    /// </summary>
    public static class TypeExtensions {
        private static readonly ConcurrentDictionary<Type, ParquetSchema> _cachedReflectedSchemas = new();

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
            if(_cachedReflectedSchemas.TryGetValue(t, out ParquetSchema? schema))
                return schema;

            schema = CreateSchema(t, forWriting);

            _cachedReflectedSchemas[t] = schema;
            return schema;
        }

        private static List<PropertyInfo> FindProperties(Type t, bool forWriting) {
            PropertyInfo[] props = t.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            return forWriting
                ? props.Where(p => p.CanWrite).ToList()
                : props.Where(p => p.CanRead).ToList();
        }

        private static Field ConstructDataField(string name, string propertyName, Type t, PropertyInfo? pi) {
            Field r;
            bool? isNullable = pi == null
                ? null
                : IsRequired(pi) ? false : null;

            if(t == typeof(DateTime)) {
                bool isTimestamp = pi?.GetCustomAttribute<ParquetTimestampAttribute>() != null;
                r = new DateTimeDataField(name,
                    isTimestamp ? DateTimeFormat.DateAndTime : DateTimeFormat.Impala,
                    isNullable, null, propertyName);
            } else if(t == typeof(TimeSpan)) {
                r = new TimeSpanDataField(name,
                    pi?.GetCustomAttribute<ParquetMicroSecondsTimeAttribute>() == null
                        ? TimeSpanFormat.MilliSeconds
                        : TimeSpanFormat.MicroSeconds,
                    isNullable, null, propertyName);
#if NET6_0_OR_GREATER
            } else if(t == typeof(TimeOnly)) {
                r = new TimeOnlyDataField(name,
                    pi?.GetCustomAttribute<ParquetMicroSecondsTimeAttribute>() == null
                        ? TimeSpanFormat.MilliSeconds
                        : TimeSpanFormat.MicroSeconds,
                    isNullable, null, propertyName);
#endif
            } else if(t == typeof(decimal)) {
                ParquetDecimalAttribute? ps = pi?.GetCustomAttribute<ParquetDecimalAttribute>();
                r = ps == null
                    ? new DecimalDataField(name,
                        DecimalFormatDefaults.DefaultPrecision, DecimalFormatDefaults.DefaultScale,
                        isNullable: isNullable, propertyName: propertyName)
                    : new DecimalDataField(name, ps.Precision, ps.Scale, isNullable: isNullable, propertyName: propertyName);
            } else {
                r = new DataField(name, t, isNullable, null, propertyName);
            }

            return r;
        }

        private static string GetColumnName(PropertyInfo pi) {

            JsonPropertyNameAttribute? stxt = pi.GetCustomAttribute<JsonPropertyNameAttribute>();

            return stxt?.Name ?? pi.Name;
        }

        private static bool ShouldIgnore(PropertyInfo pi) {
            return 
                pi.GetCustomAttribute<JsonIgnoreAttribute>() != null;
        }

        private static bool IsLegacyRepeatable(PropertyInfo pi) {
            return pi.GetCustomAttribute<ParquetSimpleRepeatableAttribute>() != null;
        }

        private static bool IsRequired(PropertyInfo pi) {
            return pi.GetCustomAttribute<ParquetRequiredAttribute>() != null;
        }

        private static int? GetPropertyOrder(PropertyInfo pi) {

#if NETCOREAPP3_1
            return null;
#else
            JsonPropertyOrderAttribute? po = pi.GetCustomAttribute<JsonPropertyOrderAttribute>();
            return po?.Order;
#endif
        }

        private static MapField ConstructMapField(string name, string propertyName,
            Type tKey, Type tValue,
            bool forWriting) {

            Type kvpType = typeof(KeyValuePair<,>).MakeGenericType(tKey, tValue);
            PropertyInfo piKey = kvpType.GetProperty("Key")!;
            PropertyInfo piValue = kvpType.GetProperty("Value")!;

            var mf = new MapField(name, MakeField(piKey, forWriting)!, MakeField(piValue, forWriting)!);
            mf.ClrPropName = propertyName;
            return mf;
        }

        private static ListField ConstructListField(string name, string propertyName,
            Type elementType,
            bool forWriting) {

            return new ListField(name, MakeField(elementType, ListField.ElementName, propertyName, null, forWriting)!);
        }

        private static Field? MakeField(PropertyInfo pi, bool forWriting) {
            if(ShouldIgnore(pi))
                return null;

            Type t = pi.PropertyType;
            string name = GetColumnName(pi);
            string propertyName = pi.Name;

            Field r = MakeField(t, name, propertyName, pi, forWriting);
            r.Order = GetPropertyOrder(pi);
            return r;
        }

        /// <summary>
        /// Makes field from property. 
        /// </summary>
        /// <param name="t">Type of property</param>
        /// <param name="columnName">Parquet file column name</param>
        /// <param name="propertyName">Class property name</param>
        /// <param name="pi">Optional <see cref="PropertyInfo"/> that can be used to get attribute metadata.</param>
        /// <param name="forWriting"></param>
        /// <returns><see cref="DataField"/> or complex field (recursively scans class). Can return null if property is explicitly marked to be ignored.</returns>
        /// <exception cref="NotImplementedException"></exception>
        private static Field MakeField(Type t, string columnName, string propertyName,
            PropertyInfo? pi,
            bool forWriting) {

            Type bt = t.IsNullable() ? t.GetNonNullable() : t;
            if(pi != null && IsLegacyRepeatable(pi) && !bt.IsGenericIDictionary() && bt.TryExtractIEnumerableType(out Type? bti)) {
                bt = bti!;
            }

            if(SchemaEncoder.IsSupported(bt)) {
                return ConstructDataField(columnName, propertyName, t, pi);
            } else if(t.TryExtractDictionaryType(out Type? tKey, out Type? tValue)) {
                return ConstructMapField(columnName, propertyName, tKey!, tValue!, forWriting);
            } else if(t.TryExtractIEnumerableType(out Type? elementType)) {
                return ConstructListField(columnName, propertyName, elementType!, forWriting);
            } else if(t.IsClass || t.IsValueType) {
                // must be a struct then (c# class or c# struct)!
                List<PropertyInfo> props = FindProperties(t, forWriting);
                Field[] fields = props
                    .Select(p => MakeField(p, forWriting))
                    .Where(f => f != null)
                    .Select(f => f!)
                    .OrderBy(f => f.Order)
                    .ToArray();

                if(fields.Length == 0)
                    throw new InvalidOperationException($"property '{propertyName}' has no fields");

                return new StructField(columnName, fields);
            }

            throw new NotImplementedException();
        }

        private static ParquetSchema CreateSchema(Type t, bool forWriting) {

            // get it all, including base class properties (may be a hierarchy)

            List<PropertyInfo> props = FindProperties(t, forWriting);
            List<Field> fields = props
                .Select(p => MakeField(p, forWriting))
                .Where(f => f != null)
                .Select(f => f!)
                .OrderBy(f => f.Order)
                .ToList();

            return new ParquetSchema(fields);
        }
    }
}