using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Parquet.Data;

namespace Parquet
{
   static class SchemaExtensions
   {
      public static IList CreateGenericList(this DataField df, IEnumerable values)
      {
         Type elementType = df.ClrType;

         //make the type nullable if it's not a class
         if (df.HasNulls && !df.IsArray)
         {
            if (!elementType.GetTypeInfo().IsClass)
            {
               elementType = typeof(Nullable<>).MakeGenericType(elementType);
            }
         }

         //create generic list instance
         Type listType = typeof(List<>);
         Type listGType = listType.MakeGenericType(elementType);

         IList result = (IList)Activator.CreateInstance(listGType);

         foreach (object value in values)
         {
            result.Add(value);
         }

         return result;
      }

   }
}
