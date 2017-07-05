using System;
using System.Collections.Generic;
using System.Text;

namespace parq.Display.Models
{
    class ConsoleFold
    {
      public IEnumerable<object[]> Rows { get; set; }
      public int IndexStart { get; set; }
      public int IndexEnd { get; set; }
   }
}
