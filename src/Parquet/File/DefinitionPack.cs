using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;

namespace Parquet.File
{
   /// <summary>
   /// Packs/unpacks definition levels
   /// </summary>
   class DefinitionPack
   {
      private readonly SchemaElement _schema;
      private readonly ParquetOptions _formatOptions;

      public DefinitionPack(SchemaElement schema, ParquetOptions formatOptions = null)
      {
         _schema = schema;
         _formatOptions = formatOptions ?? new ParquetOptions();
      }

      public IList Unpack(IList values, out List<int> definitions)
      {
         definitions = new List<int>(values.Count);
         IList result = TypeFactory.Create(_schema, false);

         foreach(object value in values)
         {
            if(value == null)
            {
               definitions.Add(0);
            }
            else
            {
               definitions.Add(_schema.MaxDefinitionLevel);
               result.Add(value);
            }
         }

         return result;
      }

      public void Pack(IList values, List<int> definitions)
      {
         if (definitions == null || !TypeFactory.IsNullable(values)) return;

         int valueIdx = 0;

         for(int i = 0; i < definitions.Count; i++)
         {
            bool isDefined = definitions[i] != 0;

            if(!isDefined)
            {
               values.Insert(valueIdx, null);
            }

            valueIdx++;
         }
      }
   }
}
