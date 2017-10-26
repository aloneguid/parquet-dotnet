using System;
using System.Collections;
using System.Collections.Generic;

namespace Parquet.Data
{
   sealed class CallbackList<T> : IList<T>
   {
      private readonly List<T> _list;

      public Func<T, T> OnAdd { get; set; }

      public Func<int, T, T> OnInsert { get; set; }

      public Action<T> OnRemove { get; set; }

      public Action<int> OnRemoveAt { get; set; }

      public Action OnClear { get; set; }

      public CallbackList()
      {
         _list = new List<T>();
      }

      public CallbackList(IEnumerable<T> collection)
      {
         _list = new List<T>(collection);
      }

      public CallbackList(int capacity)
      {
         _list = new List<T>(capacity);
      }

      #region [ IList methods ]

      public T this[int index] { get => _list[index]; set => _list[index] = value; }

      public int Count => _list.Count;

      public bool IsReadOnly => false;

      public void Add(T item)
      {
         if (OnAdd != null) item = OnAdd(item);

         _list.Add(item);
      }

      public void Clear()
      {
         OnClear?.Invoke();

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
         if (OnInsert != null) item = OnInsert(index, item);

         _list.Insert(index, item);
      }

      public bool Remove(T item)
      {
         OnRemove?.Invoke(item);

         return _list.Remove(item);
      }

      public void RemoveAt(int index)
      {
         OnRemoveAt?.Invoke(index);

         _list.RemoveAt(index);
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return _list.GetEnumerator();
      }

      #endregion
   }
}
