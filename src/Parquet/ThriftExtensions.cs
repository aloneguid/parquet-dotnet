using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.Data;

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

      public static bool IsNullable(this Thrift.SchemaElement schemaElement)
      {
         return schemaElement.Repetition_type != Thrift.FieldRepetitionType.REQUIRED;
      }

      public static string GetPath(this Thrift.ColumnChunk columnChunk)
      {
         return string.Join(Schema.PathSeparator, columnChunk.Meta_data.Path_in_schema);
      }
   }
}
