using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet.File
{
   /// <summary>
   /// Helper list class for tracking statistics:
   /// - Min
   /// - Max
   /// - Null count
   /// - Distinct count
   /// </summary>
   /// <typeparam name="T"></typeparam>
   class StatTrackingList<T> : IList<T>, IReadOnlyCollection<T>, IList
   {
      private T _min;
      private T _max;
      private int _nullCount;
      private int _distinctCount;

      private readonly List<T> _list;

      public StatTrackingList(int capacity)
      {
         _list = new List<T>(capacity);
      }

      public StatTrackingList()
      {
         _list = new List<T>();
      }

      #region [ IList Members ]

      public T this[int index] { get => _list[index]; set => _list[index] = value; }

      public int Count => _list.Count;

      public bool IsReadOnly => false;

      public bool IsFixedSize => false;

      public bool IsSynchronized => false;

      public object SyncRoot => null;

      object IList.this[int index] { get => _list[index]; set => _list[index] = (T)value; }

      public void Add(T item)
      {
         _list.Add(item);
      }

      public void Clear()
      {
         _list.Clear();
      }

      public bool Contains(T item)
      {
         return _list.Contains(item);
      }

      public void CopyTo(T[] array, int arrayIndex)
      {
         _list.CopyTo(array, arrayIndex);
      }

      public IEnumerator<T> GetEnumerator()
      {
         return _list.GetEnumerator();
      }

      public int IndexOf(T item)
      {
         return _list.IndexOf(item);
      }

      public void Insert(int index, T item)
      {
         _list.Insert(index, item);
      }

      public bool Remove(T item)
      {
         return _list.Remove(item);
      }

      public void RemoveAt(int index)
      {
         _list.RemoveAt(index);
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return _list.GetEnumerator();
      }

      public int Add(object value)
      {
         _list.Add((T)value);

         return 0;
      }

      public bool Contains(object value)
      {
         return _list.Contains((T)value);
      }

      public int IndexOf(object value)
      {
         return _list.IndexOf((T)value);
      }

      public void Insert(int index, object value)
      {
         _list.Insert(index, (T)value);
      }

      public void Remove(object value)
      {
         _list.Remove((T)value);
      }

      public void CopyTo(Array array, int index)
      {
         _list.CopyTo((T[])array, index);
      }

      #endregion
   }
}
