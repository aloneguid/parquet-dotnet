using System;
using System.Collections.Generic;
using System.Collections;
using System.Reflection;
using Parquet.Data;
using System.Linq;
using Parquet.File.Values;
using Parquet.File.Values.Primitives;

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
      }

      private static bool IsPrimitiveNullable(Type t)
      {
         TypeInfo ti = t.GetTypeInfo();

         if (ti.IsClass) return false;

         return ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(Nullable<>);
      }

      public static IList Create(Type systemType, bool nullable = false)
      {
         //make the type nullable if it's not a class
         if(nullable)
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
         Type t = TypePrimitive.GetSystemTypeBySchema(schema, options);
         return Create(t, nullable);
      }
   }
}
