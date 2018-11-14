namespace Parquet.CLI.Models.Tabular
{
   public class DisplayTable
   {
      public DisplayTable()
      {
         Rows = new TableRow[0];
      }
      public ColumnDetails[] ColumnDetails { get; set; }
      public TableRow Header { get; set; }
      public TableRow[] Rows { get; set; }
   }
}
