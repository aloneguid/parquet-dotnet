using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data.Rows
{
   /// <summary>
   /// Helps iterating over <see cref="DataColumn"/> returning either a singular value or an array if the column is repeated.
   /// </summary>
   class DataColumnEnumerator : IEnumerator
   {
      private int _position = -1;
      private readonly bool _isRepeated;
      private readonly Array _data;
      private readonly int _mrl;
      private readonly int[] _rls;
      private readonly DataField _field;
      private readonly DataColumn _dc;

      public DataColumnEnumerator(DataColumn dataColumn)
      {
         _isRepeated = dataColumn.HasRepetitions;
         _data = dataColumn.Data;
         _rls = dataColumn.RepetitionLevels;
         _field = dataColumn.Field;
         _dc = dataColumn;
         _mrl = _field.MaxRepetitionLevel;
      }

      public object Current { get; private set; }

      public DataColumn DataColumn => _dc;

      public bool MoveNext()
      {
         if ((_position + 1) >= _data.Length)
            return false;

         if(_isRepeated)
         {
            int read = Read(_position + 1, out object current);

            _position += read;

            Current = current;
         }
         else
         {
            Current = _data.GetValue(++_position);
         }

         return true;
      }

      private int Read(int position, out object cr)
      {
         //0 indicates start of a new row
         int prl = 0;
         int read = 0;
         var result = new TreeList(null);
         TreeList current = result;

         while(position < _data.Length)
         {
            int rl = _rls[position];

            if (rl == 0 && current.HasValues)
            {
               break;
            }

            int lmv = rl - prl;

            if(lmv != 0)
            {
               current = current.Submerge(lmv);
            }

            object value = _data.GetValue(position);
            current.Add(value);
            read += 1;

            prl = rl;
            position += 1;
         }

         cr = result.FirstChild?.Compact(_field.ClrNullableIfHasNullsType);
         return read;
      }

      public void Reset()
      {
         _position = -1;
      }

      public override string ToString()
      {
         return $"{_position}/{_data.Length} of {_field}";
      }
   }
}
