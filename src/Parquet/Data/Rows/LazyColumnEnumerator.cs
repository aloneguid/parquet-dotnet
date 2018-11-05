using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data.Rows
{
   class LazyColumnEnumerator : IEnumerable, IEnumerator
   {
      private readonly DataColumn _dc;
      private readonly int _startOffset;
      private int _offset;
      private readonly int _count;
      private readonly Array _data;
      private readonly int[] _rls;
      private readonly int _rl;
      private readonly int _maxRl;

      public LazyColumnEnumerator(DataColumn dc) : this(dc, 0, dc.Data.Length, 0)
      {
         //
      }

      private LazyColumnEnumerator(DataColumn dc,
         int offset, int count,
         int rl)
      {
         _dc = dc;

         _startOffset = offset;
         _offset = _startOffset - 1;
         _count = count;

         _data = dc.Data;
         _rls = dc.RepetitionLevels;
         _rl = rl;
         _maxRl = dc.Field.MaxRepetitionLevel;
      }

      public object Current { get; private set; }

      public bool MoveNext()
      {
         if ((_offset + 1) >= (_startOffset + _count))
         {
            Current = null;
            return false;
         }

         if(_rl == _maxRl)
         {
            Current = _data.GetValue(++_offset);
         }
         else
         {
            ReadCurrent();
         }

         return true;
      }

      public void Reset()
      {
         _offset = _startOffset - 1;
      }

      /// <summary>
      /// Reads current element using repetition and definition levels
      /// </summary>
      /// <returns></returns>
      private void ReadCurrent()
      {
         //get the next window where RL drops down

         int prl = -1;

         for(int i = _offset + 1; i < _startOffset + _count; i++)
         {
            int rl = _rls[i];

            if (prl != -1 && (rl != _maxRl && rl <= prl))
            {
               Current = new LazyColumnEnumerator(_dc, _offset + 1, i - (_offset + 1), _rl + 1);
               _offset = i - 1;
               return;
            }

            prl = rl;
         }

         Current = new LazyColumnEnumerator(_dc, _offset + 1, _count - (_offset + 1), _rl + 1);
         _offset = _startOffset + _count - 1;
      }

      public IEnumerator GetEnumerator()
      {
         Reset();

         return this;
      }
   }
}
