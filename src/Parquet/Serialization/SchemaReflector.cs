using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Parquet.Attributes;
using Parquet.Data;

namespace Parquet.Serialization
{
   /// <summary>
   /// Infers a class schema using reflection
   /// </summary>
   public class SchemaReflector
   {
      private readonly TypeInfo _classType;
      private static readonly ConcurrentDictionary<Type, Schema> _cachedReflectedSchemas = new ConcurrentDictionary<Type, Schema>();
      private static readonly ConcurrentDictionary<Type, Schema> _cachedReflectedWithInheritedPropertiesSchemas = new();

      /// <summary>
      /// </summary>
      public SchemaReflector(Type classType)
      {
         if (classType == null)
         {
            throw new ArgumentNullException(nameof(classType));
         }

         _classType = classType.GetTypeInfo();
      }

      /// <summary>
      /// 
      /// </summary>
      /// <returns></returns>
      public Schema Reflect()
      {
         IEnumerable<PropertyInfo> properties = _classType.DeclaredProperties.Where(pickSerializableProperties);

         return new Schema(properties.Select(GetField).Where(p => p != null).ToList());
      }

      /// <summary>
      /// TODO: i think this is the wrong cref.  how to point to non-static instance method?
      /// Same functionality as <see cref="SchemaReflector.Reflect()"/>, 
      /// but this method includes any `DeclaredProperties` inherited from 
      /// the given `classType`'s `BaseClass`
      /// TODO: instead of doing things this way, we can add a nullable "_baseClassType" attribute,
      ///         which, if not null, is used to add to the property list in the normal method. 
      ///         then, the static methods would just 
      ///            `new SchemaReflector(classType, baseClassType).Reflect()`
      ///         instead of what we have now.
      /// </summary>
      /// <returns><see cref="Schema"/></returns>
      public Schema ReflectWithInheritedProperties()
      {
         IEnumerable<PropertyInfo> properties = _classType.DeclaredProperties;
         IEnumerable<PropertyInfo> baseClassProperties = _classType.BaseType.GetTypeInfo().DeclaredProperties;

         List<Field> allValidFields = baseClassProperties.Concat(properties)
                                                         .Where(pickSerializableProperties)
                                                         .Select(GetField)
                                                         .Where(isNotNull)
                                                         .ToList();

         return new Schema(allValidFields);
      }

      /// <summary>
      /// 
      /// </summary>
      /// <typeparam name="T"></typeparam>
      /// <returns></returns>
      public static Schema Reflect<T>()
      {
         return _cachedReflectedSchemas.GetOrAdd(typeof(T), t => new SchemaReflector(typeof(T)).Reflect());
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="classType"></param>
      /// <returns></returns>
      public static Schema Reflect(Type classType)
      {
         return _cachedReflectedSchemas.GetOrAdd(classType, t => new SchemaReflector(classType).Reflect());
      }

      /// <summary>
      /// see non-static method <see cref="SchemaReflector.ReflectWithInheritedProperties()"/>
      /// </summary>
      /// <returns><see cref="Schema"/></returns>
      public static Schema ReflectWithInheritedProperties<T>()
      {
         Type classType = typeof(T);
         return ReflectWithInheritedProperties(classType);
      }

      /// <summary>
      /// see non-static method <see cref="ReflectWithInheritedProperties()"/>
      /// </summary>
      /// <param name="classType"></param>
      /// <returns><see cref="Schema"/></returns>
      public static Schema ReflectWithInheritedProperties(Type classType)
      {
         // TODO: how to differentiate between cached schemas for a type with/without inherited properties.  different cache?
         return _cachedReflectedWithInheritedPropertiesSchemas.GetOrAdd(
            classType, 
            // TODO: why do we need the `t` argument?
            t => new SchemaReflector(classType).ReflectWithInheritedProperties()
         );
      }

      private Field GetField(PropertyInfo property)
      {
         Type pt = property.PropertyType;
         if(pt.IsNullable()) pt = pt.GetNonNullable();
         if(pt.TryExtractEnumerableType(out Type t)) pt = t;

         IDataTypeHandler handler = DataTypeFactory.Match(pt);

         if (handler == null) return null;

         ParquetColumnAttribute columnAttr = property.GetCustomAttribute<ParquetColumnAttribute>();

         string name = columnAttr?.Name ?? property.Name;
         DataType type = handler.DataType;

         var r = new DataField(name,
            property.PropertyType   //use CLR type here as DF constructor will figure out nullability and other parameters
         );

         if (columnAttr != null)
         {
            if (columnAttr.UseListField)
               return new ListField(r.Name, handler.DataType, r.HasNulls, property.Name, columnAttr.ListContainerName, columnAttr.ListElementName);

            if (handler.ClrType == typeof(TimeSpan))
               r = new TimeSpanDataField(r.Name, columnAttr.TimeSpanFormat, r.HasNulls, r.IsArray);
            if (handler.ClrType == typeof(DateTime) || handler.ClrType == typeof(DateTimeOffset))
               r = new DateTimeDataField(r.Name, columnAttr.DateTimeFormat, r.HasNulls, r.IsArray);
            if (handler.ClrType == typeof(decimal))
               r = new DecimalDataField(r.Name, columnAttr.DecimalPrecision, columnAttr.DecimalScale, columnAttr.DecimalForceByteArrayEncoding, r.HasNulls, r.IsArray);
         }

         r.ClrPropName = property.Name;

         return r;
      }

      Func<PropertyInfo, bool> pickSerializableProperties = (PropertyInfo arg) => !arg.CustomAttributes.Any(p => p.AttributeType == typeof(ParquetIgnoreAttribute));

      // TODO: initially defined this using the pattern above, but didn't know the syntax for a type-generic version, so wrote this instead.  any issue with that?
      private static bool isNotNull<T>(T obj) => obj != null;
   }
}
