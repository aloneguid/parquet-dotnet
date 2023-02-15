using System;
using System.Collections;
using System.Collections.Generic;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Rows {
    class DataColumnAppender {
        private readonly DataField _dataField;
        private readonly List<object?> _values = new List<object?>();
        private readonly List<int> _rls = new List<int>();
        private readonly bool _isRepeated;
        private LevelIndex[]? _lastIndexes;

        public DataColumnAppender(DataField dataField) {
            _dataField = dataField;
            _isRepeated = dataField.MaxRepetitionLevel > 0;
        }

        public void Add(object? value, LevelIndex[] indexes) {
            if(_isRepeated) {
                int rl = GetRepetitionLevel(indexes, _lastIndexes!);

                if(!(value is string) && value is IEnumerable valueItems) {
                    int count = 0;
                    foreach(object? valueItem in (IEnumerable)value) {
                        _values.Add(valueItem);
                        _rls.Add(rl);
                        rl = _dataField.MaxRepetitionLevel;
                        count += 1;
                    }

                    if(count == 0) {
                        //handle empty collections
                        _values.Add(null);
                        _rls.Add(0);
                    }
                } else {
                    _values.Add(value);
                    _rls.Add(rl);
                }

                _lastIndexes = indexes;
            } else {
                //non-repeated fields can only appear on the first level and have no repetition levels (obviously)
                _values.Add(value);
            }

        }

        public DataColumn ToDataColumn() {
            var data = Array.CreateInstance(_dataField.ClrNullableIfHasNullsType, _values.Count);

            for(int i = 0; i < _values.Count; i++)
                data.SetValue(_values[i], i);

            return new DataColumn(_dataField, data, _isRepeated ? _rls?.ToArray() : null);
        }

        public override string ToString() => _dataField.ToString();

        private static int GetRepetitionLevel(LevelIndex[] currentIndexes, LevelIndex[] lastIndexes) {
            for(int i = 0; i < (lastIndexes?.Length ?? 0); i++)
                if(currentIndexes[i].Index != lastIndexes![i].Index)
                    return lastIndexes[i].Level;

            return 0;
        }
    }
}