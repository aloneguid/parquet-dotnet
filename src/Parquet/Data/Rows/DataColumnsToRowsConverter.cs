using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Parquet.Data.Rows
{
   class DataColumnsToRowsConverter
   {
      private readonly Dictionary<string, DataColumnEnumerator> _pathToColumn = new Dictionary<string, DataColumnEnumerator>();
      private readonly Schema _schema;
      private readonly long _totalRowRount;

      public DataColumnsToRowsConverter(Schema schema, DataColumn[] columns, long totalRowRount)
      {
         ValidateColumnsAreInSchema(schema, columns);

         foreach(DataColumn column in columns)
         {
            _pathToColumn[column.Field.Path] = new DataColumnEnumerator(column);
         }

         _schema = schema;
         _totalRowRount = totalRowRount;
      }

      public IReadOnlyCollection<Row> Convert()
      {
         var result = new List<Row>();

         ColumnsToRows(_schema.Fields, result, _totalRowRount);

         foreach(Row row in result)
         {
            row.Schema = _schema.Fields.ToArray();
         }

         return result;
      }

      private void ColumnsToRows(IReadOnlyCollection<Field> fields, List<Row> result, long rowCount)
      {
         for(int rowIndex = 0; rowCount == -1 || rowIndex < rowCount; rowIndex++)
         {
            if (!TryBuildNextRow(fields, out Row row))
               break;

            result.Add(row);
         }
      }

      private bool TryBuildNextRow(IReadOnlyCollection<Field> fields, out Row row)
      {
         var rowList = new List<object>();
         foreach(Field f in fields)
         {
            if(!TryBuildNextCell(f, out object cell))
            {
               row = null;
               return false;
            }

            rowList.Add(cell);
         }

         row = new Row(fields, rowList);
         return true;
      }

      private bool TryBuildNextCell(Field f, out object cell)
      {
         switch (f.SchemaType)
         {
            case SchemaType.Data:
               DataColumnEnumerator dce = _pathToColumn[f.Path];
               if (!dce.MoveNext())
               {
                  cell = null;
                  return false;
               }
               cell = dce.Current;

               break;
            case SchemaType.Map:
               bool mcok = TryBuildMapCell((MapField)f, out IReadOnlyList<Row> mapRow);
               cell = mapRow;
               return mcok;

            case SchemaType.Struct:
               bool scok = TryBuildStructCell((StructField)f, out Row scRow);
               cell = scRow;
               return scok;

            case SchemaType.List:
               return TryBuildListCell((ListField)f, out cell);

            default:
               throw OtherExtensions.NotImplemented(f.SchemaType.ToString());
         }

         return true;
      }

      private bool TryBuildListCell(ListField lf, out object cell)
      {
         if(!TryBuildNextCell(lf.Item, out cell))
         {
            cell = null;
            return false;
         }

         if(lf.Item.SchemaType != SchemaType.Data)
         {
            cell = RowSlicer.Rotate((Row)cell);
         }

         return true;
      }

      private bool TryBuildStructCell(StructField sf, out Row cell)
      {
         return TryBuildNextRow(sf.Fields, out cell);
      }

      private bool TryBuildMapCell(MapField mf, out IReadOnlyList<Row> rows)
      {
         if (!((mf.Key is DataField) && (mf.Value is DataField)))
            throw OtherExtensions.NotImplemented("complex maps");

         DataColumnEnumerator dceKey = _pathToColumn[mf.Key.Path];
         DataColumnEnumerator dceValue = _pathToColumn[mf.Value.Path];

         if(!TryBuildNextRow(
            new[] { dceKey.DataColumn.Field, dceValue.DataColumn.Field },
            out Row mapRow))
         {
            throw new ParquetException("a map has no corresponding row");
         }

         rows = RowSlicer.Rotate(mapRow);

         return true;
      }

      private static void ValidateColumnsAreInSchema(Schema schema, DataColumn[] columns)
      {
         DataField[] schemaFields = schema.GetDataFields();
         DataField[] passedFields = columns.Select(f => f.Field).ToArray();

         if (schemaFields.Length != passedFields.Length)
         {
            throw new ArgumentException($"schema has {schemaFields.Length} fields, but only {passedFields.Length} are passed", nameof(schema));
         }

         for (int i = 0; i < schemaFields.Length; i++)
         {
            DataField sf = schemaFields[i];
            DataField pf = schemaFields[i];

            if (!sf.Equals(pf))
            {
               throw new ArgumentException($"expected {sf} at position {i} but found {pf}");
            }
         }
      }
   }
}
