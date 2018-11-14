using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.CLI.Views.Tablular;

namespace Parquet.CLI.Models.Tabular
{
   public abstract class TableCell {
      public virtual int CellLineCount { get { return GetContentAreasInLines().Count; }  }
      public virtual ICellContent[] ContentAreas { get; set; }
      public virtual Dictionary<int, List<ICellContent>> GetContentAreasInLines()
      {
         int loadingOrdinal = 0;
         var ordinalGroup = new Dictionary<int, List<ICellContent>>();
         ordinalGroup[loadingOrdinal] = new List<ICellContent>();
         foreach (ICellContent content in this.ContentAreas)
         {
            if (content is BreakingRuleContentArea)
            {
               loadingOrdinal++;
               ordinalGroup[loadingOrdinal] = new List<ICellContent>();
               continue;
            }
            ordinalGroup[loadingOrdinal].Add(content);
         }
         return ordinalGroup;
      }
      public virtual ICellContent[] GetCellContentByLineOrdinal(int ordinal)
      {
         Dictionary<int, List<ICellContent>> ordinalGroup = GetContentAreasInLines();

         return ordinalGroup[ordinal].ToArray();
      }
      public virtual void Write(IConsoleOutputter consoleOutputter, ViewPort viewPort, int lineOrdinal, ColumnDetails columnConstraints)
      {
         WriteAllValues(consoleOutputter, viewPort, columnConstraints, lineOrdinal);
      }

      public virtual void WriteAllValues(IConsoleOutputter consoleOutputter, ViewPort viewPort, ColumnDetails columnConstraints, int lineOrdinal, bool displayNulls = true, string verticalSeparator = "|", string truncationIdentifier = "...")
      {
         foreach (ICellContent item in this.GetCellContentByLineOrdinal(lineOrdinal))
         {
            string data = columnConstraints.GetFormattedValue(item.Value, viewPort, displayNulls, verticalSeparator);

            if (IsOverlyLargeColumn(columnConstraints, viewPort, verticalSeparator))
            {
               if (data.Length > viewPort.Width)
               {
                  if (item.ForegroundColor.HasValue)
                  {
                     consoleOutputter.SetForegroundColor(item.ForegroundColor.Value);
                  }

                  consoleOutputter.Write(data.Substring(0, viewPort.Width - (verticalSeparator.Length * 2) - Environment.NewLine.Length - truncationIdentifier.Length));

                  consoleOutputter.SetForegroundColor(ConsoleColor.Yellow);
                  consoleOutputter.BackgroundColor = ConsoleColor.Black;
                  consoleOutputter.Write(truncationIdentifier);
                  consoleOutputter.ResetColor();
               }
               else
               {
                  if (item.ForegroundColor.HasValue)
                  {
                     consoleOutputter.SetForegroundColor(item.ForegroundColor.Value);
                  }
                  consoleOutputter.Write(data);
                  consoleOutputter.ResetColor();
               }
            }
            else if (data.Contains("[null]"))
            {
               consoleOutputter.SetForegroundColor(ConsoleColor.DarkGray);
               consoleOutputter.Write(data);
               consoleOutputter.ResetColor();
            }
            else
            {
               if (item.ForegroundColor.HasValue)
               {
                  consoleOutputter.SetForegroundColor(item.ForegroundColor.Value);
               }
               consoleOutputter.Write(data);
               consoleOutputter.ResetColor();
            }
         }

      }
      private bool IsOverlyLargeColumn(ColumnDetails column, ViewPort viewPort, string verticalSeparator)
      {
         return column.columnWidth + verticalSeparator.Length > viewPort.Width;
      }
   }
}
