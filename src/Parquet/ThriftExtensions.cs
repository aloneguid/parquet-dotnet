using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet
{
   /// <summary>
   /// Internal thrift data structure helpers
   /// </summary>
   static class ThriftExtensions
   {
      public static bool IsAnnotatedWithAny(this Thrift.SchemaElement schemaElement, Thrift.ConvertedType[] convertedTypes)
      {
         if (convertedTypes == null || convertedTypes.Length == 0) return false;

         return
            schemaElement.__isset.converted_type &&
            convertedTypes.Any(ct => ct == schemaElement.Converted_type);
      }
   }
}
