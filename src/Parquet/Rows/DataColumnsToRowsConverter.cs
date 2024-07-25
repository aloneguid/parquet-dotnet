﻿using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Rows {
    class DataColumnsToRowsConverter {
        private readonly ParquetSchema _schema;
        private readonly DataColumn[] _columns;
        private readonly long _totalRowRount;

        public DataColumnsToRowsConverter(ParquetSchema schema, DataColumn[] columns, long totalRowRount) {
            ValidateColumnsAreInSchema(schema, columns);

            _schema = schema;
            _columns = columns;
            _totalRowRount = totalRowRount;
        }

        public IReadOnlyCollection<Row> Convert() {
            var pathToColumn = new Dictionary<FieldPath, LazyColumnEnumerator>();
            foreach(DataColumn column in _columns) {
                var en = new LazyColumnEnumerator(column);
                en.Reset();

                pathToColumn[column.Field.Path] = en;
            }

            var result = new List<Row>();

            ColumnsToRows(_schema.Fields, pathToColumn, result, _totalRowRount);

            foreach(Row row in result)
                row.Schema = _schema.Fields.ToArray();

            return result;
        }

        private void ColumnsToRows(IReadOnlyCollection<Field> fields, Dictionary<FieldPath, LazyColumnEnumerator> pathToColumn, List<Row> result, long rowCount) {
            for(int rowIndex = 0; rowCount == -1 || rowIndex < rowCount; rowIndex++) {
                if(!TryBuildNextRow(fields, pathToColumn, out Row? row))
                    break;

                result.Add(row!);
            }
        }

        private IReadOnlyList<Row> BuildRows(IReadOnlyCollection<Field> fields, Dictionary<FieldPath, LazyColumnEnumerator> pathToColumn) {
            var rows = new List<Row>();

            while(TryBuildNextRow(fields, pathToColumn, out Row? row))
                rows.Add(row!);

            return rows;
        }

        private bool TryBuildNextRow(IReadOnlyCollection<Field> fields, Dictionary<FieldPath, LazyColumnEnumerator> pathToColumn,
            out Row? row) {
            var rowList = new List<object>();
            bool anyCellBuilt = false;

            foreach(Field f in fields) {
                if(!TryBuildNextCell(f, pathToColumn, out object? cell)) {
                    row = null;
                    continue;
                } else {
                    anyCellBuilt = true;
                }
                rowList.Add(cell!);
            }

            row = anyCellBuilt ? new Row(fields, rowList) : null;
            return anyCellBuilt;
        }

        private bool TryBuildNextCell(Field f, Dictionary<FieldPath, LazyColumnEnumerator> pathToColumn,
            out object? cell) {
            switch(f.SchemaType) {
                case SchemaType.Data:
                    LazyColumnEnumerator dce = pathToColumn[f.Path];
                    //MoveNext returns either an element or a list-like structure for columns that are repeated
                    if(!dce.MoveNext()) {
                        cell = null;
                        return false;
                    }
                    cell = dce.Current;

                    break;
                case SchemaType.Map:
                    bool mcok = TryBuildMapCell((MapField)f, pathToColumn, out IReadOnlyList<Row>? mapRow);
                    cell = mapRow;
                    return mcok;

                case SchemaType.Struct:
                    bool scok = TryBuildStructCell((StructField)f, pathToColumn, out Row? scRow);
                    cell = scRow;
                    return scok;

                case SchemaType.List:
                    return TryBuildListCell((ListField)f, pathToColumn, out cell);

                default:
                    throw OtherExtensions.NotImplemented(f.SchemaType.ToString());
            }

            return true;
        }

        private bool TryBuildListCell(ListField lf, Dictionary<FieldPath, LazyColumnEnumerator> pathToColumn, out object cell) {
            //As this is the list, all sub-columns of this list have to be cut into. This is essentially a more complicated version of
            //the TryBuildMapCell method

            var nestedPathTicks = pathToColumn
               .Where(ptc => ptc.Key.ToString().StartsWith(lf.Path.ToString()))
               .Select(ptc => new { path = ptc.Key, collection = ptc.Value, moved = ptc.Value.MoveNext() })
               .ToList();

            if(nestedPathTicks.Any(t => !t.moved)) {
                cell = new Row[0];
                return true;
            }

            var nestedPathToColumn = nestedPathTicks
               .ToDictionary(ptc => ptc.path, ptc => (LazyColumnEnumerator)ptc.collection.Current!);

            IReadOnlyList<Row> rows = BuildRows(new[] { lf.Item }, nestedPathToColumn);

            cell = rows.Select(r => r[0]).ToArray();

            return true;
        }

        private bool TryBuildStructCell(StructField sf, Dictionary<FieldPath, LazyColumnEnumerator> pathToColumn, out Row? cell) {
            return TryBuildNextRow(sf.Fields, pathToColumn, out cell);
        }

        private bool TryBuildMapCell(MapField mf, Dictionary<FieldPath, LazyColumnEnumerator> pathToColumn,
            out IReadOnlyList<Row>? rows) {
            //"cut into" the keys and values collection
            LazyColumnEnumerator keysCollection = pathToColumn[mf.Key.Path];
            LazyColumnEnumerator valuesCollection = pathToColumn[mf.Value.Path];

            if(keysCollection.MoveNext() && valuesCollection.MoveNext()) {
                var ptc = new Dictionary<FieldPath, LazyColumnEnumerator> {
                    [mf.Key.Path] = (LazyColumnEnumerator)keysCollection.Current!,
                    [mf.Value.Path] = (LazyColumnEnumerator)valuesCollection.Current!
                };

                rows = BuildRows(new[] { mf.Key, mf.Value }, ptc);
                return true;
            }

            rows = null;
            return false;
        }

        private static void ValidateColumnsAreInSchema(ParquetSchema schema, DataColumn[] columns) {
            DataField[] schemaFields = schema.GetDataFields();
            DataField[] passedFields = columns.Select(f => f.Field).ToArray();

            if(schemaFields.Length != passedFields.Length)
                throw new ArgumentException($"schema has {schemaFields.Length} fields, but only {passedFields.Length} are passed", nameof(schema));

            for(int i = 0; i < schemaFields.Length; i++) {
                DataField sf = schemaFields[i];
                DataField pf = schemaFields[i];

                if(!sf.Equals(pf))
                    throw new ArgumentException($"expected {sf} at position {i} but found {pf}");
            }
        }
    }
}