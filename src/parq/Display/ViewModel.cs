using System;
using System.Collections.Generic;
using System.Text;

namespace parq.Display
{
    public class ViewModel
    {
      public IEnumerable<ColumnDetails> Columns { get; set; }
      public object RawValue { get; set; }
      public List<object[]> Rows { get; internal set; }
      public long RowCount { get; internal set; }
   }
}
