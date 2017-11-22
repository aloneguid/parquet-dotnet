using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Data
{
   static class RowExtractor
   {
      public static Row Extract(IEnumerable<Field> fields, int index, Dictionary<string, IList> columns)
      {
         return new Row(fields.Select(se => CreateElement(se, index, columns)));
      }

      private static object CreateElement(Field field, int index, Dictionary<string, IList> columns)
      {
         if (field.SchemaType == SchemaType.Map)
         {
            return ((MapField)field).CreateCellValue(columns, index);
         }
         else if (field.SchemaType == SchemaType.Structure)
         {
            return Extract(((StructField)field).Fields, index, columns);
         }
         else if (field.SchemaType == SchemaType.List)
         {
            ListField lf = (ListField)field;

            if (lf.Item.SchemaType == SchemaType.Structure)
            {
               StructField structField = (StructField)lf.Item;
               Dictionary<string, IList> elementColumns = CreateFieldColumns(structField.Fields, index, columns, out int count);

               var rows = new List<Row>(count);
               for (int i = 0; i < count; i++)
               {
                  Row row = Extract(structField.Fields, i, elementColumns);
                  rows.Add(row);
               }

               return rows;
            }
            else if(lf.Item.SchemaType == SchemaType.PrimitiveType)
            {
               DataField dataField = (DataField)lf.Item;
               IList values = GetFieldPathValues(dataField, index, columns);
               return values;
            }
            else
            {
               throw OtherExtensions.NotImplementedForPotentialAssholesAndMoaners($"reading {lf.Item.SchemaType} from lists");
            }
         }
         else
         {
            if (!columns.TryGetValue(field.Path, out IList values))
            {
               throw new ParquetException($"something terrible happened, there is no column by name '{field.Name}' and path '{field.Path}'");
            }

            return values[index];
         }
      }

      private static Dictionary<string, IList> CreateFieldColumns(
         IEnumerable<Field> fields, int index,
         Dictionary<string, IList> pathToValues,
         out int count)
      {
         var elementPathToValues = new Dictionary<string, IList>();

         count = int.MaxValue;

         foreach (Field field in fields)
         {
            string key = field.Path;
            IList value = pathToValues[key][index] as IList;
            elementPathToValues[key] = value;
            if (value.Count < count) count = value.Count;
         }

         return elementPathToValues;
      }

      private static IList GetFieldPathValues(Field field, int index, Dictionary<string, IList> columns)
      {
         IList values = columns[field.Path][index] as IList;
         return values;
      }

   }
}
