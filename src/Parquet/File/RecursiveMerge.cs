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
            Row row = CreateRow(_schema.Elements, i, pathToValues);
            ds.Add(row);
         }

         return ds;
      }

      private Row CreateRow(IEnumerable<SchemaElement> schema, int rowIdx, Dictionary<string, IList> pathToValues)
      {
         var values = new List<object>();

         foreach(SchemaElement se in schema)
         {
            object value;

            if (se.IsNestedStructure)
            {
               value = se.IsRepeated
                  ? (object)CreateRows(se.Children, rowIdx, pathToValues)
                  : (object)CreateRow(se.Children, rowIdx, pathToValues);
            }
            else
            {
               IList column = pathToValues[se.Path];
               value = column[rowIdx];
            }

            values.Add(value);
         }

         return new Row(values);
      }

      private ICollection<Row> CreateRows(IEnumerable<SchemaElement> schema, int rowIdx, Dictionary<string, IList> pathToValues)
      {
         var list = new List<Row>();

         var nestedPathToValues = schema
            .ToDictionary(se => se.Path, se => pathToValues[se.Path] as IList);

         int count = nestedPathToValues.Min(e => e.Value.Count);

         for (int i = 0; i < count; i++)
         {
            Row row = CreateRow(schema, i, nestedPathToValues);
            list.Add(row);
         }

         return list;
      }
   
   }
}
