using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data
{
   /// <summary>
   /// Experimental:
   /// Wraps source list in repetiton flags and returns elements on request
   /// </summary>
   class ListWithRepetitions<T> : IList<T>
   {
      private readonly List<T> _flatList;
      private readonly int _maxRepetitionLevel;
      private readonly Dictionary<int, KeyValuePair<int, int>> _indexToWindow = new Dictionary<int, KeyValuePair<int, int>>();

      public ListWithRepetitions(List<T> flatList, List<int> repetitions, int maxRepetitionLevel)
      {
         _flatList = flatList;
         _maxRepetitionLevel = maxRepetitionLevel;

         FillWindows(repetitions);
      }

      private void FillWindows(List<int> levels)
      {
         for(int i = 0; i < _flatList.Count; i++)
         {
            int level = levels[i];
            
         }
      }

      #region [ IList members ]

      public T this[int index] { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

      public int Count => _indexToWindow.Count;

      public bool IsReadOnly => false;

      public void Add(T item)
      {
         throw new NotImplementedException();
      }

      public void Clear()
      {
         throw new NotImplementedException();
      }

      public bool Contains(T item)
      {
         throw new NotImplementedException();
      }

      public void CopyTo(T[] array, int arrayIndex)
      {
         throw new NotImplementedException();
      }

      public IEnumerator<T> GetEnumerator()
      {
         throw new NotImplementedException();
      }

      public int IndexOf(T item)
      {
         throw new NotImplementedException();
      }

      public void Insert(int index, T item)
      {
         throw new NotImplementedException();
      }

      public bool Remove(T item)
      {
         throw new NotImplementedException();
      }

      public void RemoveAt(int index)
      {
         throw new NotImplementedException();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         throw new NotImplementedException();
      }

      #endregion
   }
}