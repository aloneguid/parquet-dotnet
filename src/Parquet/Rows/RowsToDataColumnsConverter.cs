using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Rows {
    class RowsToDataColumnsConverter {
        private readonly ParquetSchema _schema;
        private readonly IReadOnlyCollection<Row> _rows;
        private readonly Dictionary<FieldPath, DataColumnAppender> _pathToDataColumn = new();

        public RowsToDataColumnsConverter(ParquetSchema schema, IReadOnlyCollection<Row> rows) {
            _schema = schema;
            _rows = rows;
        }

        public IReadOnlyCollection<DataColumn> Convert() {
            ProcessRows(_schema.Fields, _rows, 0, Array.Empty<LevelIndex>());

            var result = _schema.GetDataFields()
               .Select(df => GetAppender(df).ToDataColumn())
               .ToList();

            return result;
        }

        private void ProcessRows(IReadOnlyCollection<Field> fields, IReadOnlyCollection<Row>? rows, int level, LevelIndex[] indexes) {
            if(rows == null)
                return;

            int i = 0;
            foreach(Row row in rows)
                ProcessRow(fields, row, level, indexes.Append(new LevelIndex(level, i++)));
        }

        private void ProcessRow(IReadOnlyCollection<Field> fields, Row? row, int level, LevelIndex[] indexes) {
            int cellIndex = 0;
            foreach(Field f in fields) {
                switch(f.SchemaType) {
                    case SchemaType.Data:
                        ProcessDataValue(f, row == null ? null : row[cellIndex], indexes);
                        break;

                    case SchemaType.Map:
                        ProcessMap((MapField)f, row == null ? null : (IReadOnlyCollection<Row>)row[cellIndex]!, level + 1, indexes);
                        break;

                    case SchemaType.Struct:
                        ProcessRow(((StructField)f).Fields, row == null ? null : (Row)row[cellIndex]!, level + 1, indexes);
                        break;

                    case SchemaType.List:
                        ProcessList((ListField)f, row == null ? null : row[cellIndex]!, level + 1, indexes);
                        break;

                    default:
                        throw new NotImplementedException();
                }

                cellIndex++;
            }
        }

        private void ProcessMap(MapField mapField, IReadOnlyCollection<Row>? mapRows, int level, LevelIndex[] indexes) {
            var fields = new Field[] { mapField.Key, mapField.Value };

            List<object?> keyCell = mapRows == null ? new List<object?>() : mapRows.Select(r => r[0]).ToList();
            List<object?> valueCell = mapRows == null ? new List<object?>() : mapRows.Select(r => r[1]).ToList();
            var row = new Row(keyCell, valueCell);

            ProcessRow(fields, row, level, indexes);
        }

        private void ProcessList(ListField listField, object? cellValue, int level, LevelIndex[] indexes) {
            Field f = listField.Item;

            switch(f.SchemaType) {
                case SchemaType.Data:
                    //list has a special case for simple elements where they are not wrapped in rows
                    ProcessDataValue(f, cellValue, indexes);
                    break;
                case SchemaType.Struct:
                    ProcessRows(((StructField)f).Fields, cellValue == null ? null : (IReadOnlyCollection<Row>)cellValue, level, indexes);
                    break;
                default:
                    throw new NotSupportedException();
            }
        }

        private void ProcessDataValue(Field f, object? value, LevelIndex[] indexes) {
            GetAppender(f).Add(value, indexes);
        }

        private DataColumnAppender GetAppender(Field f) {
            //prepare value appender
            if(!_pathToDataColumn.TryGetValue(f.Path, out DataColumnAppender? appender)) {
                appender = new DataColumnAppender((DataField)f);
                _pathToDataColumn[f.Path] = appender;
            }

            return appender;
        }
    }
}