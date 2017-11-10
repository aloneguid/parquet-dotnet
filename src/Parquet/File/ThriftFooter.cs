using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.File
{
   class ThriftFooter
   {
      private readonly Thrift.FileMetaData _fileMeta;

      public ThriftFooter(Thrift.FileMetaData fileMeta)
      {
         _fileMeta = fileMeta;
      }

      public Thrift.SchemaElement GetSchemaElement(Thrift.ColumnChunk columnChunk)
      {
         if (columnChunk == null)
         {
            throw new ArgumentNullException(nameof(columnChunk));
         }

         List<string> path = columnChunk.Meta_data.Path_in_schema;

         int i = 0;
         foreach (string pp in path)
         {
            while(i < _fileMeta.Schema.Count)
            {
               if (_fileMeta.Schema[i].Name == pp) break;

               i++;
            }
         }

         return _fileMeta.Schema[i];
      }

      public void GetLevels(Thrift.ColumnChunk columnChunk, out int maxRepetitionLevel, out int maxDefinitionLevel)
      {
         maxRepetitionLevel = 0;
         maxDefinitionLevel = 0;

         int i = 0;
         List<string> path = columnChunk.Meta_data.Path_in_schema;

         foreach (string pp in path)
         {
            while(i < _fileMeta.Schema.Count)
            {
               if(_fileMeta.Schema[i].Name == pp)
               {
                  Thrift.SchemaElement se = _fileMeta.Schema[i];

                  bool repeated = (se.__isset.repetition_type && se.Repetition_type == Thrift.FieldRepetitionType.REPEATED);
                  bool defined = (se.Repetition_type != Thrift.FieldRepetitionType.REQUIRED);

                  if (repeated) maxRepetitionLevel += 1;
                  if (defined) maxDefinitionLevel += 1;

                  break;
               }

               i++;
            }
         }
      }
   }
}
