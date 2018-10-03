using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Data.Rows
{
   class RowsToDataColumnsConverter
   {
      private readonly Schema _schema;
      private readonly IReadOnlyCollection<Row> _rows;
      private readonly Dictionary<string, DataColumnAppender> _pathToDataColumn = new Dictionary<string, DataColumnAppender>();

      public RowsToDataColumnsConverter(Schema schema, IReadOnlyCollection<Row> rows)
      {
         _schema = schema;
         _rows = rows;
      }

      public IReadOnlyCollection<DataColumn> Convert()
      {
         ProcessRows(_schema.Fields, _rows, 0);

         List<DataColumn> result = _schema.GetDataFields()
            .Select(df => _pathToDataColumn[df.Path].ToDataColumn())
            .ToList();

         return result;
      }

      private void ProcessRows(IReadOnlyCollection<Field> fields, IReadOnlyCollection<Row> rows, int level)
      {
         int i = 0;
         foreach(Row row in rows)
         {
            ProcessRow(fields, row, level, i++);
         }
      }

      private void ProcessRow(IReadOnlyCollection<Field> fields, Row row, int level, int index)
      {
         int cellIndex = 0;
         foreach(Field f in fields)
         {
            switch (f.SchemaType)
            {
               case SchemaType.Data:
                  ProcessDataValue(f, row[cellIndex], level, index);
                  break;

               case SchemaType.Map:
                  ProcessMap((MapField)f, (IReadOnlyCollection<Row>)row[cellIndex], level + 1);
                  break;

               case SchemaType.Struct:
                  ProcessRow(((StructField)f).Fields, (Row)row[cellIndex], level + 1, index);
                  break;

               case SchemaType.List:
                  ProcessList((ListField)f, row[cellIndex], level + 1, index);
                  break;

               default:
                  throw new NotImplementedException();
            }

            cellIndex++;
         }
      }

      private void ProcessMap(MapField mapField, IReadOnlyCollection<Row> mapRows, int level)
      {
         var fields = new Field[] { mapField.Key, mapField.Value };

         var keyCell = mapRows.Select(r => r[0]).ToList();
         var valueCell = mapRows.Select(r => r[1]).ToList();
         var row = new Row(keyCell, valueCell);

         ProcessRow(fields, row, level, 0);
      }

      private void ProcessList(ListField listField, object cellValue, int level, int index)
      {
         Field f = listField.Item;

         switch (f.SchemaType)
         {
            case SchemaType.Data:
               //list has a special case for simple elements where they are not wrapped in rows
               ProcessDataValue(f, cellValue, level, index);
               break;
            case SchemaType.Struct:
               ProcessRows(((StructField)f).Fields, (IReadOnlyCollection<Row>)cellValue, level + 1);
               break;
            default:
               throw new NotSupportedException();
         }
      }

      private void ProcessDataValue(Field f, object value, int level, int index)
      {
         //prepare value appender
         if(!_pathToDataColumn.TryGetValue(f.Path, out DataColumnAppender appender))
         {
            appender = new DataColumnAppender((DataField)f);
            _pathToDataColumn[f.Path] = appender;
         }

         appender.Add(value, level, index);
      }
   }
}
