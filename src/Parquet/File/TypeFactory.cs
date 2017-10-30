using System;
using System.Collections.Generic;
using System.Collections;
using System.Reflection;
using Parquet.Data;

namespace Parquet.File
{
   static class TypeFactory
   {
      public static IList Create(Type systemType, bool nullable = false, bool repeated = false, int? capacity = null)
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

         if (capacity == null)
         {
            return (IList)Activator.CreateInstance(listGType);
         }

         return (IList)Activator.CreateInstance(listGType, capacity.Value);
      }

      public static IList Create(SchemaElement schema, ParquetOptions options, bool nullable = false, int? capacity = null)
      {
         //Type t = TypePrimitive.GetSystemTypeBySchema(schema, options);
         Type t = schema.ElementType;  //use ElementType as it's properly pre-calculated
         return Create(t, nullable, false, capacity);
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
