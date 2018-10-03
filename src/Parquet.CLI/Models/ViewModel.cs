using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Parquet.Data.Rows;

namespace Parquet.CLI.Models
{
   public class ViewModel
   {
      public Table DataTable { get; internal set; }
      public IEnumerable<ColumnDetails> Columns { get; internal set; }
      public List<object[]> Rows { get; internal set; }
      public int RowCount { get; internal set; }
      public Schema Schema { get; internal set; }
   }
}
