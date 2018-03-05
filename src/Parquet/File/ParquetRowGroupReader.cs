using System;
using Parquet.Data;

namespace Parquet.File
{
   internal class ParquetRowGroupReader
   {
      private readonly Thrift.RowGroup _rowGroup;

      internal ParquetRowGroupReader(Thrift.RowGroup rowGroup)
      {
         _rowGroup = rowGroup ?? throw new ArgumentNullException(nameof(rowGroup));
      }

      /// <summary>
      /// Gets the number of rows in this row group
      /// </summary>
      public long RowCount => _rowGroup.Num_rows;

      /// <summary>
      /// Reads a column from this row group.
      /// </summary>
      /// <param name="field"></param>
      /// <returns></returns>
      public DataColumn ReadColumn(DataField field)
      {
         throw new NotImplementedException();
      }
   }
}