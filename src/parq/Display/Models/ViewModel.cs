using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Text;

namespace parq.Display.Models
{
    public class ViewModel
    {
      public IEnumerable<ColumnDetails> Columns { get; set; }
      public object RawValue { get; set; }
      public List<object[]> Rows { get; internal set; }
      public long RowCount { get; internal set; }
      public Schema Schema { get; internal set; }
   }
}
