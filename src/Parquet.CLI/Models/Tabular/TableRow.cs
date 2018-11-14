using System.Linq;

namespace Parquet.CLI.Models.Tabular
{
   public class TableRow {
      public TableCell[] Cells { get; set; }
      public int MaxCellLineCount { get { return Cells.Max(c => c.CellLineCount); } }
   }
}
