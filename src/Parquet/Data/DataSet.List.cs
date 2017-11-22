using System;
using System.Collections;
using System.Collections.Generic;

namespace Parquet.Data
{
   public partial class DataSet : IList<Row>
   {
      /// <summary>
      /// Gets or sets row by index
      /// </summary>
      /// <param name="index">Row index</param>
      /// <returns>Row object</returns>
      public Row this[int index]
      {
         get => CreateRow(index);
         set => throw OtherExtensions.NotImplementedForPotentialAssholesAndMoaners("assigning row by index");
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
      public int FieldCount => _schema.Fields.Count;

      /// <summary>
      /// Gets a value indicating whether the <see cref="T:System.Collections.Generic.ICollection`1" /> is read-only.
      /// </summary>
      public bool IsReadOnly => false;

      /// <summary>
      /// Adds an item to the <see cref="T:System.Collections.Generic.ICollection`1" />.
      /// </summary>
      /// <param name="item">The object to add to the <see cref="T:System.Collections.Generic.ICollection`1" />.</param>
      public void Add(Row item)
      {
         AddRow(item);
      }

      /// <summary>
      /// Clears data in the dataset
      /// </summary>
      public void Clear()
      {
         foreach(KeyValuePair<string, IList> kvp in _columns)
         {
            kvp.Value.Clear();
         }

         _rowCount = 0;
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

      /// <summary>
      /// Gets enumerator I guess
      /// </summary>
      /// <returns></returns>
      public IEnumerator<Row> GetEnumerator()
      {
         throw OtherExtensions.NotImplementedForPotentialAssholesAndMoaners("enumerating DataSet");
      }

      /// <summary>
      /// This operation is not supported
      /// </summary>
      public int IndexOf(Row item)
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// Inserts a new row, not implemented
      /// </summary>
      /// <param name="index"></param>
      /// <param name="item"></param>
      public void Insert(int index, Row item)
      {
         throw OtherExtensions.NotImplementedForPotentialAssholesAndMoaners("inserting rows into DataSet");
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
         throw OtherExtensions.NotImplementedForPotentialAssholesAndMoaners("enumerating DataSet");
      }
   }
}
