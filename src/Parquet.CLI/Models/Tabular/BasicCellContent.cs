using System;

namespace Parquet.CLI.Models.Tabular
{
   public class BasicCellContent : ICellContent
   {
      public object Value { get; set; }
      public ConsoleColor? ForegroundColor { get;  set; }
   }
}
