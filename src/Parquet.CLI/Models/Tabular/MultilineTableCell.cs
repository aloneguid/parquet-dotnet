using System.Linq;

namespace Parquet.CLI.Models.Tabular
{
   public class MultilineTableCell : TableCell
   {
      public override int CellLineCount { get => this.ContentAreas.OfType<BreakingRuleContentArea>().Count() + 1; }
   }
}
