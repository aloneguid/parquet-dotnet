using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.CLI.Models;
using Parquet.CLI.Models.Tabular;

namespace Parquet.CLI.Views.Tablular
{
   public interface IConsoleOutputter
   {
      void Write(string s);
      void SetForegroundColor(ConsoleColor foreColor);
      ConsoleColor BackgroundColor { get; set; }
      void ResetColor();
   }

   public class TableWriter
   {
      private const string verticalSeparator = "|";
      private const string horizontalSeparator = "-";
      private const string cellDivider = "+";
      private readonly IConsoleOutputter consoleOutputter;
      private readonly ViewPort viewPort;

      public TableWriter(ViewPort viewPort) : this (new ConsoleOutputter(), viewPort) { }
      public TableWriter(IConsoleOutputter consoleOutputter, ViewPort viewPort)
      {
         this.consoleOutputter = consoleOutputter;
         this.viewPort = viewPort;
      }
      private bool IsOverlyLargeColumn(ColumnDetails column, ViewPort viewPort)
      {
         return column.columnWidth + verticalSeparator.Length > viewPort.Width;
      }

      public void Draw(DisplayTable table)
      {
         if (table.Header.Cells.Any())
         {
            DrawLine(table, viewPort);
            WriteHeaderLine(table, viewPort);
            DrawLine(table, viewPort);
         }
         if (table.Rows.Any())
         {
            WriteValues(table, viewPort, true, "...", false);
            DrawLine(table, viewPort);
         }
      }

      private void WriteHeaderLine(DisplayTable table, ViewPort viewPort, bool displayTypes=false)
      {

         for (int j = 0; j < table.Header.MaxCellLineCount; j++)
         {
            consoleOutputter.Write(verticalSeparator);
            for (int h = 0; h < table.Header.Cells.Length; h++)
            {
               ColumnDetails column = table.ColumnDetails[h];
               TableCell cell = table.Header.Cells[h];
               

               cell.Write(consoleOutputter, viewPort, j, column);
               consoleOutputter.Write(verticalSeparator);

            }
            consoleOutputter.Write(Environment.NewLine);
         }

         // todo: the idea of a TableCell is to handle this
         /*if (displayTypes)
         {
            consoleOutputter.Write(verticalSeparator);
            foreach (ColumnDetails column in sheet.Columns)
            {
               int offset = column.isNullable ? 1 : 0;
               if (IsOverlyLargeColumn(column, viewPort))
               {
                  for (int i = 0; i < viewPort.Width - offset - column.type.ToString().Length - (verticalSeparator.Length * 2) - Environment.NewLine.Length; i++)
                  {
                     consoleOutputter.Write(" ");
                  }
               }
               else
               {
                  for (int i = 0; i < column.columnWidth - column.type.ToString().Length - offset; i++)
                  {
                     consoleOutputter.Write(" ");
                  }
               }
               consoleOutputter.ForegroundColor = ConsoleColor.Blue;

               if (column.columnWidth > column.type.ToString().Length + (verticalSeparator.Length * 2))
               {
                  consoleOutputter.Write(column.type.ToString());
               }
               else
               {
                  int howMuchSpaceDoWeHave = column.isNullable ? column.columnWidth - 1 : column.columnWidth;
                  consoleOutputter.Write(column.type.ToString().Substring(0, howMuchSpaceDoWeHave));
               }


               if (column.isNullable)
               {
                  consoleOutputter.ForegroundColor = ConsoleColor.Cyan;
                  consoleOutputter.Write("?");
                  consoleOutputter.ResetColor();
               }
               consoleOutputter.Write(verticalSeparator);
            }
            consoleOutputter.Write(Environment.NewLine);
         }*/
      }

      private void WriteValues(DisplayTable table, ViewPort viewPort, bool displayNulls = true, string truncationIdentifier = "...", bool displayRefs=false)
      {
         for (int i = 0; i < table.Rows.Length; i++)
         {
            TableRow row = table.Rows.ElementAt(i);
            consoleOutputter.Write(verticalSeparator);
            for (int j = 0; j < row.Cells.Length; j++)
            {
               ColumnDetails header = table.ColumnDetails.ElementAt(j);
               TableCell cell = row.Cells[j];

               cell.Write(consoleOutputter, viewPort, 0, header);

               consoleOutputter.Write(verticalSeparator);
               
            }
            consoleOutputter.Write(Environment.NewLine);
         }
      }
      private void DrawLine(DisplayTable displayTable, ViewPort viewPort, bool drawRefs = false)
      {
         consoleOutputter.Write(cellDivider);
         for (int c = 0; c < displayTable.Header.Cells.Length; c++)
         {
            TableCell column = displayTable.Header.Cells[c];
            ColumnDetails details = displayTable.ColumnDetails[c];

            int columnNameLength = c.ToString().Length;
            if (IsOverlyLargeColumn(details, viewPort))
            {
               for (int i = 0; i < ((viewPort.Width - (verticalSeparator.Length * 2) - Environment.NewLine.Length) / 2) - (columnNameLength / 2); i++)
               {
                  consoleOutputter.Write(horizontalSeparator);
               }
               if (drawRefs)
               {
                  consoleOutputter.Write(c.ToString());
               }
               else
               {
                  consoleOutputter.Write(horizontalSeparator);
               }
               for (int i = 0; i < ((viewPort.Width - (verticalSeparator.Length * 2) - Environment.NewLine.Length) / 2) - (columnNameLength / 2) - ((columnNameLength + 1) % 2); i++)
               {
                  consoleOutputter.Write(horizontalSeparator);
               }
            }
            else
            {
               for (int i = 0; i < (details.columnWidth / 2) - (c.ToString().Length / 2); i++)
               {
                  consoleOutputter.Write(horizontalSeparator);
               }
               if (drawRefs)
               {
                  consoleOutputter.Write(c.ToString());
               }
               else
               {
                  consoleOutputter.Write(horizontalSeparator);
               }
               for (int i = 0; i < (details.columnWidth / 2) - (c.ToString().Length / 2) - ((details.columnWidth + 1) % 2); i++)
               {
                  consoleOutputter.Write(horizontalSeparator);
               }
            }
            consoleOutputter.Write(cellDivider);
         }
         consoleOutputter.Write(Environment.NewLine);
      }

      
   }
}
