using System;

namespace Parquet.CLI.Models.Tabular
{
   public class BreakingRuleContentArea : ICellContent
   {
      public object Value { get; set; }
      public ConsoleColor? ForegroundColor { get; set; }
   }
}
