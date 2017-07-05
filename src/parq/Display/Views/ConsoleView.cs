using System;
using System.Collections.Generic;
using System.Linq;

namespace parq.Display.Views
{
   public class ConsoleView
    {
      private const string verticalSeparator = "|";
      private const string horizontalSeparator = "-";
      private Stack<ConsoleSheet> unreadSheets;
      private Stack<ConsoleSheet> readSheets = new Stack<ConsoleSheet>();
      private ConsoleSheet currentSheet;

      public void Draw(ViewModel viewModel)
      { 
         Console.Clear();
         var viewPort = new ViewPort();

         unreadSheets = GenerateSheets(viewPort, viewModel.Columns);

         currentSheet = unreadSheets.Pop();
         DrawSheet(viewModel, currentSheet);
         
      }

      private void DrawSheet(ViewModel viewModel, ConsoleSheet currentSheet)
      {
         DrawLine(currentSheet);
         WriteHeaderLine(currentSheet);
         DrawLine(currentSheet);
         WriteValues(viewModel, currentSheet);
         DrawLine(currentSheet);
         WriteSummary(viewModel, currentSheet);

         var input = AwaitInput();
         switch (input)
         {
            case Input.NoOp:
               DrawSheet(viewModel, currentSheet);
               break;
            case Input.Quit:
               break;
            case Input.NextPage:
               if (unreadSheets.Any())
               {
                  readSheets.Push(currentSheet);
                  var nextPage = unreadSheets.Pop();
                  DrawSheet(viewModel, nextPage);
               }
               else
               {
                  DrawSheet(viewModel, currentSheet);
               }
               break;
            case Input.PrevPage:
               if (readSheets.Any())
               {
                  unreadSheets.Push(currentSheet);
                  var lastPage = readSheets.Pop();
                  DrawSheet(viewModel, lastPage);
               }
               else
               {
                  DrawSheet(viewModel, currentSheet);
               }
               break;
         }
      }

      private Stack<ConsoleSheet> GenerateSheets(ViewPort viewPort, IEnumerable<ColumnDetails> columns)
      {
         var pages = new Stack<ConsoleSheet>();
         var firstPage = FitColumns(viewPort, columns);
         var sheet = new ConsoleSheet { Columns = firstPage, IndexStart = 0, IndexEnd = firstPage.Count() };
         pages.Push(sheet);
         
         var runningTotal = firstPage.Count();
         while (runningTotal != columns.Count())
         {
            var nextPage = FitColumns(viewPort, columns.Skip(runningTotal));
            var nextSheet = new ConsoleSheet { Columns = nextPage, IndexStart = runningTotal, IndexEnd = runningTotal + nextPage.Count() };
            runningTotal += nextPage.Count();
            pages.Push(nextSheet);
         }

         int originalPageCount = pages.Count;
         var unReadPages = new Stack<ConsoleSheet>();
         for (int i = 0; i < originalPageCount; i++)
         {
            unReadPages.Push(pages.Pop());
         }
         return unReadPages;
      }

      private Input AwaitInput()
      {
         var key = Console.ReadKey();
         if (key.Key == ConsoleKey.RightArrow)
         {
            return Input.NextPage;
         }
         else if (key.Key == ConsoleKey.LeftArrow)
         {
            return Input.PrevPage;
         }
         else if (key.Key == ConsoleKey.Enter)
         {
            return Input.Quit;
         }

         return Input.NoOp;
      }

      private IEnumerable<ColumnDetails> FitColumns(ViewPort viewPort, IEnumerable<ColumnDetails> columns)
      {
         var allColumnsWidth = columns
            .Select(col => (col.columnWidth < AppSettings.Instance.DisplayMinWidth.Value ? AppSettings.Instance.DisplayMinWidth.Value : col.columnWidth) + verticalSeparator.Length)
            .Sum() + verticalSeparator.Length;

         if (allColumnsWidth > viewPort.Width)
         {
            int runningTotal = 0;
            List<ColumnDetails> chosenColumns = new List<ColumnDetails>();
            foreach (var column in columns)
            {
               if (runningTotal  + column.columnWidth + verticalSeparator.Length > viewPort.Width)
               {
                  return chosenColumns;
               }
               runningTotal += column.columnWidth + verticalSeparator.Length;
               chosenColumns.Add(column);
            }
         }

         return columns;
      }

      private void WriteSummary(ViewModel viewModel, ConsoleSheet currentSheet)
      {
         Console.WriteLine("Showing {0} to {1} of {2} Columns. Press <- or -> to navigate.", currentSheet.IndexStart, currentSheet.IndexEnd, viewModel.Columns.Count());
         Console.WriteLine("{0} Rows Affected. Press ENTER to quit;", viewModel.RowCount);
      }
      private void WriteHeaderLine(ConsoleSheet columnDetails)
      {
         Console.Write(verticalSeparator);
         foreach (string name in columnDetails.Columns.Select(cd => cd.columnName))
         {
            if (name.Length < AppSettings.Instance.DisplayMinWidth.Value)
            {
               for (int i = 0; i < AppSettings.Instance.DisplayMinWidth.Value - name.Length; i++)
               {
                  Console.Write(" ");
               }
            }
            Console.Write(name);
            Console.Write(verticalSeparator);
         }
         Console.Write(Environment.NewLine);
      }

      private void WriteValues(ViewModel viewModel, ConsoleSheet columnsFitToScreen)
      {
         for (int i = 0; i < viewModel.Rows.Count; i++)
         {
            var row = viewModel.Rows[i];
            Console.Write(verticalSeparator);
            for (int j = 0; j < row.Length; j++)
            {
               var header = viewModel.Columns.ElementAt(j);
               if (columnsFitToScreen.Columns.Contains(header))
               {
                  Console.Write(header.GetFormattedValue(row[j]));
                  Console.Write(verticalSeparator);
               }
            }
            Console.WriteLine();
         }
      }
      private void DrawLine(ConsoleSheet columnHeaderSizes)
      {
         Console.Write(verticalSeparator);
         foreach (int item in columnHeaderSizes.Columns.Select(d => d.columnWidth))
         {
            for (int i = 0; i < item; i++)
            {
               Console.Write(horizontalSeparator);
            }
            Console.Write(verticalSeparator);
         }
         Console.Write(Environment.NewLine);
      }
   }
}
