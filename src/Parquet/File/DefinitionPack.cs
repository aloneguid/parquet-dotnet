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

      public IList Pack(IList condensedValues, List<int> definitions)
      {
         if (definitions == null) return condensedValues;

         int valueIdx = 0;
         IList values = TypeFactory.Create(_schema, _formatOptions, true);

         //todo: check condensedValues and definitions boundaries at the same time
         foreach (int isDefinedInt in definitions)
         {
            bool isDefined = isDefinedInt != 0;

            if (isDefined)
            {
               values.Add(condensedValues[valueIdx++]);
            }
            else
            {
               values.Add(null);
            }
         }

         return values;
      }
   }
}
