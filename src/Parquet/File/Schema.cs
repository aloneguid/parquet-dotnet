using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Thrift;

namespace Parquet.File
{
   /// <summary>
   /// Represents file schema
   /// </summary>
   class Schema
   {
      private readonly Dictionary<string, SchemaElement> _pathToElement = new Dictionary<string, SchemaElement>();

      public Schema(FileMetaData fileMeta)
      {
         Build(fileMeta);
      }

      public SchemaElement this[ColumnChunk cc]
      {
         get
         {
            //todo: support inline columns
            return _pathToElement[cc.Meta_data.Path_in_schema[0]];
         }
      }

      public int GetMaxDefinitionLevel(ColumnChunk cc)
      {
         int max = 0;

         foreach(string part in cc.Meta_data.Path_in_schema)
         {
            SchemaElement element = _pathToElement[part];
            if (element.Repetition_type != FieldRepetitionType.REQUIRED) max += 1;
         }

         return max;
      }

      private void Build(FileMetaData fileMeta)
      {
         foreach(SchemaElement se in fileMeta.Schema)
         {
            _pathToElement[se.Name] = se;
         }
      }
   }
}
