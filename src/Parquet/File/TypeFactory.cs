using System;
using System.Collections.Generic;
using System.Collections;
using System.Reflection;
using Parquet.Data;

namespace Parquet.File
{
   static class TypeFactory
   {
      public static void AdjustSchema(Thrift.SchemaElement schema, Type systemType)
      {
         if(IsPrimitiveNullable(systemType))
         {
            throw new ArgumentException($"Type '{systemType}' in column '{schema.Name}' is a nullable type. Please pass either a class or a primitive type.", nameof(systemType));
         }

         TypePrimitive tp = TypePrimitive.Find(systemType);

         schema.Type = tp.ThriftType;

         if (tp.ThriftAnnotation != null)
         {
            schema.Converted_type = tp.ThriftAnnotation.Value;
         }

         //todo: not the best place for it, but it's a special case at the moment
         if(systemType == typeof(decimal))
         {
            schema.Precision = 38;
            schema.Scale = 18;
         }
      }

      private static bool IsPrimitiveNullable(Type t)
      {
         TypeInfo ti = t.GetTypeInfo();

         if (ti.IsClass) return false;

         return ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(Nullable<>);
      }

      public static IList Create(Type systemType, bool nullable = false, bool repeated = false)
      {
         //make the type nullable if it's not a class
         if(nullable && !repeated)
         {
            if(!systemType.GetTypeInfo().IsClass)
            {
               systemType = typeof(Nullable<>).MakeGenericType(systemType);
            }
         }

         //create generic list instance
         Type listType = typeof(List<>);
         Type listGType = listType.MakeGenericType(systemType);
         return (IList)Activator.CreateInstance(listGType);
      }

      public static IList Create(SchemaElement schema, ParquetOptions options, bool nullable = false)
      {
         //Type t = TypePrimitive.GetSystemTypeBySchema(schema, options);
         Type t = schema.ElementType;  //use ElementType as it's properly pre-calculated
         return Create(t, nullable);
      }

      public static bool TryExtractEnumerableType(Type t, out Type baseType)
      {
         TypeInfo ti = t.GetTypeInfo();

         if(ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(IEnumerable<>))
         {
            baseType = ti.GenericTypeArguments[0];
            return true;
         }

         baseType = null;
         return false;
      }
   }
}
