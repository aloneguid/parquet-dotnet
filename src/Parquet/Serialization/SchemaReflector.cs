using System;
using System.Collections;
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

      private Field GetField(PropertyInfo property)
      {
         Type pt = property.PropertyType;
         if (pt.IsNullable())
         {
            pt = pt.GetNonNullable();
         }

         Type prop = property.PropertyType;
         bool underlyingTypeIsCollection = typeof(Array).IsAssignableFrom(prop) || (prop.IsGenericType && typeof(IEnumerable).IsAssignableFrom(prop));
         if (pt.IsArray || underlyingTypeIsCollection)
         {
            pt = pt.HasElementType ? pt.GetElementType() : pt.GetGenericArguments()[0];
         }

         IDataTypeHandler handler = DataTypeFactory.Match(pt);

         if (handler == null)
         {
            return null;
         }

         ParquetColumnAttribute columnAttr = property.GetCustomAttribute<ParquetColumnAttribute>();

         string name = columnAttr?.Name ?? property.Name;
         DataType type = handler.DataType;

         var r = new DataField(name,
            property.PropertyType   //use CLR type here as DF constructor will figure out nullability and other parameters
         );

         if (columnAttr != null)
         {
            if (columnAttr.UseListField)
            {
               return new ListField(r.Name, handler.DataType, r.HasNulls, property.Name, columnAttr.ListContainerName, columnAttr.ListElementName);
            }

            if (handler.ClrType == typeof(TimeSpan))
            {
               r = new TimeSpanDataField(r.Name, columnAttr.TimeSpanFormat, r.HasNulls, r.IsArray);
            }

            if (handler.ClrType == typeof(DateTime) || handler.ClrType == typeof(DateTimeOffset))
            {
               r = new DateTimeDataField(r.Name, columnAttr.DateTimeFormat, r.HasNulls, r.IsArray);
            }

            if (handler.ClrType == typeof(decimal))
            {
               r = new DecimalDataField(r.Name, columnAttr.DecimalPrecision, columnAttr.DecimalScale, columnAttr.DecimalForceByteArrayEncoding, r.HasNulls, r.IsArray);
            }
         }

         r.ClrPropName = property.Name;

         return r;
      }

      private readonly Func<PropertyInfo, bool> pickSerializableProperties = (PropertyInfo arg) => !arg.CustomAttributes.Any(p => p.AttributeType == typeof(ParquetIgnoreAttribute));

   }
}
