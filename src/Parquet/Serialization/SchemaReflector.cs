using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Serialization {
    /// <summary>
    /// Infers a class schema using reflection
    /// </summary>
    public class SchemaReflector {
        private readonly TypeInfo _classType;
        private static readonly ConcurrentDictionary<Type, ParquetSchema> _cachedReflectedSchemas = new();

        private static readonly ConcurrentDictionary<Type, ParquetSchema>
            _cachedReflectedWithInheritedPropertiesSchemas = new();

        /// <summary>
        /// </summary>
        public SchemaReflector(Type classType) {
            if(classType == null) {
                throw new ArgumentNullException(nameof(classType));
            }

            _classType = classType.GetTypeInfo();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ParquetSchema Reflect() {
            IEnumerable<PropertyInfo> properties = _classType.DeclaredProperties.Where(pickSerializableProperties);

            return new ParquetSchema(properties.Select(GetField).Where(p => p != null).ToList());
        }

        /// <summary>
        /// Same functionality as <see cref="Reflect()"/>, 
        /// but this method includes any `DeclaredProperties` inherited from 
        /// the given `classType`'s `BaseClass`
        /// TODO: instead of doing things this way, we can add a nullable "_baseClassType" attribute,
        ///         which, if not null, is used to add to the property list in the normal method. 
        ///         then, the static methods would just 
        ///            `new SchemaReflector(classType, baseClassType).Reflect()`
        ///         instead of what we have now.
        /// </summary>
        /// <returns><see cref="Schema"/></returns>
        public ParquetSchema ReflectWithInheritedProperties() {
            IEnumerable<PropertyInfo> properties = _classType.DeclaredProperties;
            IEnumerable<PropertyInfo> baseClassProperties = _classType.BaseType.GetTypeInfo().DeclaredProperties;

            List<Field> allValidFields = baseClassProperties.Concat(properties)
                .Where(pickSerializableProperties)
                .Select(GetField)
                .Where(isNotNull)
                .ToList();

            return new ParquetSchema(allValidFields);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static ParquetSchema Reflect<T>() {
            return _cachedReflectedSchemas.GetOrAdd(typeof(T), t => new SchemaReflector(typeof(T)).Reflect());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="classType"></param>
        /// <returns></returns>
        public static ParquetSchema Reflect(Type classType) {
            return _cachedReflectedSchemas.GetOrAdd(classType, t => new SchemaReflector(classType).Reflect());
        }

        /// <summary>
        /// see non-static method <see cref="O:ReflectWithInheritedProperties"/>
        /// </summary>
        /// <returns><see cref="Schema"/></returns>
        public static ParquetSchema ReflectWithInheritedProperties<T>() {
            Type classType = typeof(T);
            return ReflectWithInheritedProperties(classType);
        }

        /// <summary>
        /// see non-static method <see cref="O:ReflectWithInheritedProperties"/>
        /// </summary>
        /// <param name="classType"></param>
        /// <returns><see cref="Schema"/></returns>
        public static ParquetSchema ReflectWithInheritedProperties(Type classType) {
            // TODO: how to differentiate between cached schemas for a type with/without inherited properties.  different cache?
            return _cachedReflectedWithInheritedPropertiesSchemas.GetOrAdd(
                classType,
                // TODO: why do we need the `t` argument?
                t => new SchemaReflector(classType).ReflectWithInheritedProperties()
            );
        }

        private Field GetField(PropertyInfo property) {
            Type pt = property.PropertyType;
            if(pt.IsNullable()) pt = pt.GetNonNullable();
            if(pt.TryExtractEnumerableType(out Type t)) pt = t;

            DataType? dataType = SchemaEncoder.FindDataType(pt);
            if(dataType == null) return null;

            ParquetColumnAttribute columnAttr = property.GetCustomAttribute<ParquetColumnAttribute>();

            string name = columnAttr?.Name ?? property.Name;

            var r = new DataField(name,
                property.PropertyType //use CLR type here as DF constructor will figure out nullability and other parameters
            );

            if(columnAttr != null) {
                if(columnAttr.UseListField)
                    return new ListField(r.Name, dataType.Value, r.IsNullable, property.Name,
                        columnAttr.ListContainerName, columnAttr.ListElementName);

                if(pt == typeof(TimeSpan))
                    r = new TimeSpanDataField(r.Name, columnAttr.TimeSpanFormat, r.IsNullable, r.IsArray);
                if(pt == typeof(DateTime) || pt == typeof(DateTimeOffset))
                    r = new DateTimeDataField(r.Name, columnAttr.DateTimeFormat, r.IsNullable, r.IsArray);
                if(pt == typeof(decimal))
                    r = new DecimalDataField(r.Name, columnAttr.DecimalPrecision, columnAttr.DecimalScale,
                        columnAttr.DecimalForceByteArrayEncoding, r.IsNullable, r.IsArray);
            }

            r.ClrPropName = property.Name;

            return r;
        }

        private readonly Func<PropertyInfo, bool> pickSerializableProperties = (PropertyInfo arg) =>
            !arg.CustomAttributes.Any(p => p.AttributeType == typeof(ParquetIgnoreAttribute));

        // TODO: initially defined this using the pattern above, but didn't know the syntax for a type-generic version, so wrote this instead.  any issue with that?
        private static bool isNotNull<T>(T obj) => obj != null;
    }
}