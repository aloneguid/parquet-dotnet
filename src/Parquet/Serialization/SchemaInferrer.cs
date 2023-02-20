using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Parquet.Encodings;
using Parquet.Schema;
using Parquet.Serialization.Attributes;
using PIA = Parquet.Serialization.Attributes.ParquetIgnoreAttribute;

namespace Parquet.Serialization {
    class SchemaInferrer {
        private readonly Type _objectType;

        public SchemaInferrer(Type objectType) {
            _objectType = objectType;
        }

        public ParquetSchema CreateSchema(bool forWriting) {

            // get it all, including base class properties (may be a hierarchy)

            List<PropertyInfo> props = FindProperties(_objectType, forWriting);
            List<Field> fields = props.Select(p=> MakeField(p, forWriting)).Where(f => f != null).Select(f => f!).ToList();

            return new ParquetSchema(fields);
        }

        private List<PropertyInfo> FindProperties(Type t, bool forWriting) {
            PropertyInfo[] props = t.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            return forWriting
                ? props.Where(p => p.CanWrite).ToList()
                : props.Where(p => p.CanRead).ToList();
        }

        private DataField ConstructDataField(string name, string propertyName, Type t, PropertyInfo pi) {
            var df = new DataField(name, t, propertyName: propertyName);

            // todo: adjust using metadata

            return df;
        }

        /// <summary>
        /// Makes field from property. 
        /// </summary>
        /// <param name="pi"></param>
        /// <param name="forWriting"></param>
        /// <returns><see cref="DataField"/> or complex field (recursively scans class). Can return null if property is explicitly marked to be ignored.</returns>
        /// <exception cref="NotImplementedException"></exception>
        private Field? MakeField(PropertyInfo pi, bool forWriting) {
            bool ignore = pi.GetCustomAttribute<PIA>() != null;
            if(ignore)
                return null;

            Type t = pi.PropertyType;
            string name = pi.GetCustomAttribute<ParquetPropertyNameAttribute>()?.Name ?? pi.Name;

            Type bt = t.IsNullable() ? t.GetNonNullable() : t;
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
    }

    class SchemaInferrer<T> : SchemaInferrer where T : class {
        public SchemaInferrer() : base(typeof(T)) { 
        }
    }
}
