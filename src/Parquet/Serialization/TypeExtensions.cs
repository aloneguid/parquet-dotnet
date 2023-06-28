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
        private static readonly ConcurrentDictionary<(Type, ParquetCompabilityOptions), ParquetSchema> _cachedReflectedSchemas = new();

        /// <summary>
        /// Reflects this type to get <see cref="ParquetSchema"/>
        /// </summary>
        /// <param name="t"></param>
        /// <param name="forWriting"> 
        /// Set to true to get schema when deserialising into classes (writing to classes), otherwise false.
        /// The result will differ if for instance some properties are read-only and some write-only.
        /// </param>
        /// <param name="options"></param>
        /// <returns></returns>
        public static ParquetSchema GetParquetSchema(this Type t, bool forWriting, ParquetCompabilityOptions options = default) {
            if(_cachedReflectedSchemas.TryGetValue((t, options), out ParquetSchema? schema))
                return schema;

            schema = CreateSchema(t, forWriting, options);

            _cachedReflectedSchemas[(t, options)] = schema;
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

            if(t == typeof(DateTime)) {
                bool isTimestamp = pi?.GetCustomAttribute<ParquetTimestampAttribute>() != null;
                r = new DateTimeDataField(name,
                    isTimestamp ? DateTimeFormat.DateAndTime : DateTimeFormat.Impala,
                    propertyName: propertyName);
            } else if(t == typeof(TimeSpan)) {
                r = new TimeSpanDataField(name,
                    pi?.GetCustomAttribute<ParquetMicroSecondsTimeAttribute>() == null
                        ? TimeSpanFormat.MilliSeconds
                        : TimeSpanFormat.MicroSeconds,
                    propertyName: propertyName);
#if NET6_0_OR_GREATER
            } else if(t == typeof(TimeOnly)) {
                r = new TimeOnlyDataField(name,
                    pi?.GetCustomAttribute<ParquetMicroSecondsTimeAttribute>() == null
                        ? TimeSpanFormat.MilliSeconds
                        : TimeSpanFormat.MicroSeconds,
                    propertyName: propertyName);
#endif
            } else if(t == typeof(decimal)) {
                ParquetDecimalAttribute? ps = pi?.GetCustomAttribute<ParquetDecimalAttribute>();
                r = ps == null
                    ? new DecimalDataField(name,
                        DecimalFormatDefaults.DefaultPrecision, DecimalFormatDefaults.DefaultScale,
                        propertyName: propertyName)
                    : new DecimalDataField(name, ps.Precision, ps.Scale, propertyName: propertyName);
            } else if(t == typeof(string)) {
                bool? nullable = pi?.IsNullable();
                r = new DataField(name, t, nullable, propertyName: propertyName);
            } else {
                r = new DataField(name, t, propertyName: propertyName);
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
            bool forWriting,
            ParquetCompabilityOptions options) {

            Type kvpType = typeof(KeyValuePair<,>).MakeGenericType(tKey, tValue);
            PropertyInfo piKey = kvpType.GetProperty("Key")!;
            PropertyInfo piValue = kvpType.GetProperty("Value")!;

            var mf = new MapField(name, MakeField(piKey, forWriting, options)!, MakeField(piValue, forWriting, options)!);
            mf.ClrPropName = propertyName;
            return mf;
        }

        private static ListField ConstructListField(string name, string propertyName,
            Type elementType,
            bool forWriting,
            ParquetCompabilityOptions options) {

            return new ListField(name, MakeField(elementType, ListField.ElementName, propertyName, null, forWriting, options)!);
        }

        private static Field? MakeField(PropertyInfo pi, bool forWriting, ParquetCompabilityOptions options) {
            if(ShouldIgnore(pi))
                return null;

            Type t = pi.PropertyType;
            string name = GetColumnName(pi);
            string propertyName = pi.Name;

            Field r = MakeField(t, name, propertyName, pi, forWriting, options);
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
        /// <param name="options"></param>
        /// <returns><see cref="DataField"/> or complex field (recursively scans class). Can return null if property is explicitly marked to be ignored.</returns>
        /// <exception cref="NotImplementedException"></exception>
        private static Field MakeField(Type t, string columnName, string propertyName,
            PropertyInfo? pi,
            bool forWriting,
            ParquetCompabilityOptions options) {

            Type bt = t.IsNullable() ? t.GetNonNullable() : t;
            if(options.IsMakeArraysSet() && !bt.IsGenericIDictionary() && bt.TryExtractIEnumerableType(out Type? bti)) {
                bt = bti!;
            }

            if(SchemaEncoder.IsSupported(bt)) {
                return ConstructDataField(columnName, propertyName, t, pi);
            } else if(t.TryExtractDictionaryType(out Type? tKey, out Type? tValue)) {
                return ConstructMapField(columnName, propertyName, tKey!, tValue!, forWriting, options);
            } else if(t.TryExtractIEnumerableType(out Type? elementType)) {
                return ConstructListField(columnName, propertyName, elementType!, forWriting, options);
            } else if(t.IsClass || t.IsValueType) {
                // must be a struct then (c# class or c# struct)!
                List<PropertyInfo> props = FindProperties(t, forWriting);
                Field[] fields = props
                    .Select(p => MakeField(p, forWriting, options))
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

        private static ParquetSchema CreateSchema(Type t, bool forWriting, ParquetCompabilityOptions options) {

            // get it all, including base class properties (may be a hierarchy)

            List<PropertyInfo> props = FindProperties(t, forWriting);
            List<Field> fields = props
                .Select(p => MakeField(p, forWriting, options))
                .Where(f => f != null)
                .Select(f => f!)
                .OrderBy(f => f.Order)
                .ToList();

            return new ParquetSchema(fields);
        }
    }
}