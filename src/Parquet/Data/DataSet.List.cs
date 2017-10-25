using System;
using System.Collections;
using System.Collections.Generic;

namespace Parquet.Data
{
   public partial class DataSet : IList<Row>
   {
      public Row this[int index]
      {
         get => CreateRow(index);
         set => throw new NotImplementedException();
      }

      /// <summary>
      /// Gets the number of rows contained in this dataset
      /// </summary>
      public int Count => _rowCount;

      /// <summary>
      /// Gets the number of rows contained in this dataset.
      /// </summary>
      public int RowCount => _rowCount;

      /// <summary>
      /// Gets the number of columns contained in this dataset
      /// </summary>
      public int ColumnCount => _schema.Elements.Count;

      /// <summary>
      /// Gets a value indicating whether the <see cref="T:System.Collections.Generic.ICollection`1" /> is read-only.
      /// </summary>
      public bool IsReadOnly => false;

      /// <summary>
      /// Adds an item to the <see cref="T:System.Collections.Generic.ICollection`1" />.
      /// </summary>
      /// <param name="row">The object to add to the <see cref="T:System.Collections.Generic.ICollection`1" />.</param>
      public void Add(Row item)
      {
         AddRow(item);
      }

      public void Clear()
      {

      }

      /// <summary>
      /// This operation is not supported
      /// </summary>
      public bool Contains(Row item)
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// This operation is not supported
      /// </summary>
      public void CopyTo(Row[] array, int arrayIndex)
      {
         for(int i = 0; i < Count; i++)
         {
            Row row = CreateRow(i);
            array[arrayIndex++] = row;
         }
      }

      public IEnumerator<Row> GetEnumerator()
      {
         throw new NotImplementedException();
      }

      /// <summary>
      /// This operation is not supported
      /// </summary>
      public int IndexOf(Row item)
      {
         throw new NotSupportedException();
      }

      public void Insert(int index, Row item)
      {
         throw new NotImplementedException();
      }

      /// <summary>
      /// This operation is not supported
      /// </summary>
      public bool Remove(Row item)
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// Removes the <see cref="T:System.Collections.Generic.IList`1" /> item at the specified index.
      /// </summary>
      /// <param name="index">The zero-based index of the item to remove.</param>
      public void RemoveAt(int index)
      {
         RemoveRow(index);
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         throw new NotImplementedException();
      }
   }
}
