using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data.Rows
{
   class DataColumnAppender
   {
      private readonly DataField _dataField;
      private readonly List<object> _values = new List<object>();
      private readonly List<int> _rls = new List<int>();
      private readonly bool _isRepeated;
      private int _lastIndex;
      private int _lastLevel;

      public DataColumnAppender(DataField dataField)
      {
         _dataField = dataField;
         _isRepeated = dataField.MaxRepetitionLevel > 0;
      }

      public void Add(object value, int level, int index)
      {
         if (_isRepeated)
         {
            if(!(value is string) && value is IEnumerable valueItems)
            {
               int rl = 0;
               int count = 0;
               foreach (object valueItem in (IEnumerable)value)
               {
                  _values.Add(valueItem);
                  _rls.Add(rl);
                  rl = _dataField.MaxRepetitionLevel;
                  count += 1;
               }

               if(count == 0)
               {
                  //handle empty collections
                  _values.Add(null);
                  _rls.Add(0);
               }
            }
            else
            {
               int rl = index == 0 ? 0 : _dataField.MaxRepetitionLevel;

               _values.Add(value);
               _rls.Add(rl);
            }

            _lastLevel = level;
            _lastIndex = index;
         }
         else
         {
            //non-repeated fields can only appear on the first level and have no repetition levels (obviously)
            _values.Add(value);
         }

      }

      public DataColumn ToDataColumn()
      {
         Array data = Array.CreateInstance(_dataField.ClrNullableIfHasNullsType, _values.Count);

         for(int i = 0; i < _values.Count; i++)
         {
            data.SetValue(_values[i], i);
         }

         return new DataColumn(_dataField, data, _isRepeated ? _rls.ToArray() : null);
      }

      public override string ToString() => _dataField.ToString();
   }
}