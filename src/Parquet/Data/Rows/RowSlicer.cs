using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Parquet.Data.Rows
{
   /// <summary>
   /// Slices the rows into more human-readable format
   /// </summary>
   static class RowSlicer
   {
      public static IReadOnlyList<Row> Rotate(Row row)
      {
         var rows = new List<Row>();

         IEnumerator[] source = row.Values
            .Select(v => (IEnumerable)v)
            .Select(i => i.GetEnumerator())
            .ToArray();

         while (source.All(i => i.MoveNext()))
         {
            var irow = new Row(row.Schema, source.Select(i => i.Current).ToArray());

            rows.Add(irow);
         }

         return rows;
      }

   }
}
