using System;
using System.Collections.Generic;
using System.Text;

namespace parq.Display.Models
{
    public class ConsoleSheet
    {
      public IEnumerable<ColumnDetails> Columns { get; set; }
      public int IndexStart { get; set; }
      public int IndexEnd { get; set; }
   }
}
