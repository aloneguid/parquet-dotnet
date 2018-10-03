using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.CLI.Models
{
   public class ViewSettings
   {
      public int displayMinWidth { get; set; }
      public bool displayTypes { get; set; }
      public bool expandCells { get; set; }
      public bool displayNulls { get; set; }
      public string truncationIdentifier { get; set; }
   }
}
