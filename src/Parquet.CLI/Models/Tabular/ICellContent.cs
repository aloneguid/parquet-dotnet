using System;

namespace Parquet.CLI.Models.Tabular
{
   public interface ICellContent
   {
      object Value { get; set; }
      ConsoleColor? ForegroundColor { get; set; }
   }
}
