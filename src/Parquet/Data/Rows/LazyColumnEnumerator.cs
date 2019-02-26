using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data.Rows
{
   class LazyColumnEnumerator : IEnumerable, IEnumerator
   {
      private readonly DataColumn _dc;
      private readonly int _start;
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

         _start = offset;
         _offset = _start;
         _count = count;

         _data = dc.Data;
         _rls = dc.RepetitionLevels;
         _rl = rl;
         _maxRl = dc.Field.MaxRepetitionLevel;
      }

      public object Current { get; private set; }

      /// <summary>
      /// Helper method to get all elements as a list of enumerators
      /// </summary>
      /// <returns></returns>
      public List<LazyColumnEnumerator> ToEnumeratorList()
      {
         Reset();
         var result = new List<LazyColumnEnumerator>();
         while(MoveNext())
         {
            result.Add((LazyColumnEnumerator)Current);
         }
         return result;
      }

      public Array ToDataArray()
      {
         Array result = Array.CreateInstance(_dc.Field.ClrNullableIfHasNullsType, _count);

         int i = 0;
         Reset();
         while(MoveNext())
         {
            result.SetValue(Current, i++);
         }

         return result;
      }

      public bool MoveNext()
      {
         if (_offset >= (_start + _count))
         {
            Current = null;
            return false;
         }

         if(_rl == _maxRl)
         {
            //If you've reached the maximum repetition level the only thing you can do is read
            //actual data from the allocated window.
            //This gets invoked immediately for flat scalar types.
            Current = _data.GetValue(_offset++);
         }
         else
         {
            ReadWindow();
         }

         return true;
      }

      public void Reset()
      {
         _offset = _start;
      }

      /// <summary>
      /// Reads current element using repetition and definition levels. Given "current" repetiton level
      /// skips over the nest level to create a "window" over appropriate nesting level. This creates an illusion of
      /// nested enumerators so that you can build data structures from honestly flat parquet columns.
      /// </summary>
      /// <returns></returns>
      private void ReadWindow()
      {
         //get the next window where RL drops down

         int prl = -1;

         for (int i = _offset; i < _start + _count; i++)
         {
            int rl = _rls[i];

            if (prl != -1 && rl != _maxRl && rl == _rl)
            {
               int count = i - _offset;

               //detecting empty list is not really possible in parquet
               //we make an assumption that if RL is 0, list has one element and data element is zero it's empty
               //it's a parquet format limitation
               bool isEmpty = 
                  count == 1 &&
                  rl == 0 &&
                  (_data.GetValue(_offset) == null);

               if (isEmpty)
               {
                  count = 0;
               }

               Current = new LazyColumnEnumerator(_dc,
                  _offset,
                  count,
                  _rl + 1);
               _offset = i;
               return;
            }

            prl = rl;
         }

         int finalCount = _count - (_offset - _start);
         if(finalCount == 1 && prl == 0 && (_data.GetValue(_offset) == null))
         {
            finalCount = 0;
         }

         Current = new LazyColumnEnumerator(_dc,
            _offset,
            finalCount,
            _rl + 1);
         _offset = _start + _count; //make it big
      }

      public IEnumerator GetEnumerator()
      {
         Reset();

         return this;
      }
   }
}
