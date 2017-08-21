using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.Data;

namespace Parquet.File
{
   /// <summary>
   /// Merges results into flat <see cref="DataSet"/>
   /// </summary>
   class RecursiveMerge
   {
      private readonly Schema _schema;

      public RecursiveMerge(Schema schema)
      {
         _schema = schema ?? throw new ArgumentNullException(nameof(schema));
      }

      public DataSet Merge(Dictionary<string, IList> pathToValues)
      {
         int count = pathToValues.Min(e => e.Value.Count);

         var ds = new DataSet(_schema);

         for(int i = 0; i < count; i++)
         {
            Row row = CreateRow(i, pathToValues);
            ds.Add(row);
         }

         return ds;
      }

      internal Row CreateRow(int rowIdx, Dictionary<string, IList> pathToValues)
      {
         var values = new List<object>();

         foreach(SchemaElement se in _schema.Elements)
         {
            IList column = pathToValues[se.Path];
            object value = column[rowIdx];
            values.Add(value);
         }

         return new Row(values);
      }
   
   }
}
