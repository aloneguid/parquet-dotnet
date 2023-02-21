using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Parquet.Encodings;
using Parquet.Schema;

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

        private static Field ConstructDataField(string name, string propertyName, Type t, PropertyInfo pi) {
            var r = new DataField(name, t, propertyName: propertyName);

            ParquetColumnAttribute? columnAttr = pi.GetCustomAttribute<ParquetColumnAttribute>();

            if(columnAttr != null) {
                if(columnAttr.UseListField) {
                    return new ListField(r.Name, r.ClrNullableIfHasNullsType, propertyName,
                        columnAttr.ListContainerName, columnAttr.ListElementName);
                }

                if(t == typeof(TimeSpan))
                    r = new TimeSpanDataField(r.Name, columnAttr.TimeSpanFormat, r.IsNullable);
                else if(t == typeof(DateTime))
                    r = new DateTimeDataField(r.Name, columnAttr.DateTimeFormat, r.IsNullable);
                else if(t == typeof(decimal))
                    r = new DecimalDataField(r.Name, columnAttr.DecimalPrecision, columnAttr.DecimalScale,
                        columnAttr.DecimalForceByteArrayEncoding, r.IsNullable);
            }

            return r;
        }

        /// <summary>
        /// Makes field from property. 
        /// </summary>
        /// <param name="pi"></param>
        /// <param name="forWriting"></param>
        /// <returns><see cref="DataField"/> or complex field (recursively scans class). Can return null if property is explicitly marked to be ignored.</returns>
        /// <exception cref="NotImplementedException"></exception>
        private static Field? MakeField(PropertyInfo pi, bool forWriting) {
            bool ignore = pi.GetCustomAttribute<ParquetIgnoreAttribute>() != null;
            if(ignore)
                return null;

            Type t = pi.PropertyType;
            string name = pi.GetCustomAttribute<ParquetColumnAttribute>()?.Name ?? pi.Name;

            Type bt = t.IsNullable() ? t.GetNonNullable() : t;
            if(bt.TryExtractEnumerableType(out Type? bti)) {
                bt = bti!;
            }

            if(SchemaEncoder.IsSupported(bt)) {
                return ConstructDataField(name, pi.Name, t, pi);
            } else if(t.TryExtractDictionaryType(out Type? tKey, out Type? tValue)) {
                throw new NotImplementedException("dictionaries!");
            } else if(t.TryExtractEnumerableType(out Type? elementType)) {
                throw new NotImplementedException("lists!");
            } else if(t.IsClass) {
                // must be a struct then!
                List<PropertyInfo> props = FindProperties(t, forWriting);
                Field[] fields = props.Select(p => MakeField(p, forWriting)).Where(f => f != null).Select(f => f!).ToArray();

                if(fields.Length == 0)
                    return null;

                return new StructField(name, fields);
            }

            throw new NotImplementedException();
        }

        private static ParquetSchema CreateSchema(Type t, bool forWriting) {

            // get it all, including base class properties (may be a hierarchy)

            List<PropertyInfo> props = FindProperties(t, forWriting);
            List<Field> fields = props.Select(p => MakeField(p, forWriting)).Where(f => f != null).Select(f => f!).ToList();

            return new ParquetSchema(fields);
        }
    }
}