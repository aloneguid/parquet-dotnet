using parq.Display.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace parq.Display.Views
{
   /// <summary>
   /// Displays to the Console window a navigatable (2D) data set
   /// </summary>
   public class InteractiveConsoleView
    {
      private const string verticalSeparator = "|";
      private const string horizontalSeparator = "-";
      private Stack<ConsoleSheet> unreadSheets;
      private Stack<ConsoleSheet> readSheets = new Stack<ConsoleSheet>();
      private Stack<ConsoleFold> unreadFolds;
      private Stack<ConsoleFold> readFolds = new Stack<ConsoleFold>();

      public void Draw(ViewModel viewModel)
      { 
         Console.Clear();
         var viewPort = new ViewPort();

         unreadSheets = GenerateSheets(viewPort, viewModel.Columns);
         unreadFolds = GenerateFolds(viewPort, viewModel.Rows);

         var currentSheet = unreadSheets.Pop();
         var currentFold = unreadFolds.Pop();
         DrawSheet(viewModel, currentSheet, currentFold, viewPort);
         
      }

      private Stack<ConsoleFold> GenerateFolds(ViewPort viewPort, List<object[]> rows)
      {
         var foldLength = viewPort.Height - 7;

         var folds = new Stack<ConsoleFold>();
         var firstFoldPoint = (rows.Count > foldLength) ? foldLength : rows.Count;
         var runningTotal = firstFoldPoint;
         var firstFold = rows.Take(firstFoldPoint);

         folds.Push(new ConsoleFold { Rows = firstFold, IndexStart = 0, IndexEnd = firstFold.Count() });

         while (runningTotal != rows.Count)
         {
            var nextFoldPoint = (rows.Count > runningTotal + foldLength) ? foldLength : rows.Count - runningTotal;
            var nextFold = rows.Skip(runningTotal).Take(nextFoldPoint);
            folds.Push(new ConsoleFold { Rows = nextFold, IndexStart = runningTotal, IndexEnd = runningTotal + nextFold.Count() });
            runningTotal += nextFold.Count();
         }

         var originalFoldCount = folds.Count;
         var unReadFolds = new Stack<ConsoleFold>();
         for (int i = 0; i < originalFoldCount; i++)
         {
            unReadFolds.Push(folds.Pop());
         }
         return unReadFolds;
      }

      private void DrawSheet(ViewModel viewModel, ConsoleSheet currentSheet, ConsoleFold currentFold,  ViewPort viewPort)
      {
         Console.Clear();
         DrawLine(currentSheet);
         WriteHeaderLine(currentSheet);
         DrawLine(currentSheet);
         WriteValues(viewModel, currentSheet, currentFold);
         DrawLine(currentSheet);
         WriteSummary(viewModel, currentSheet, currentFold);

         var input = AwaitInput();
         switch (input)
         {
            case Input.NoOp:
               DrawSheet(viewModel, currentSheet, currentFold, viewPort);
               break;
            case Input.Quit:
               break;
            case Input.NextSheet:
               if (unreadSheets.Any())
               {
                  readSheets.Push(currentSheet);
                  var nextPage = unreadSheets.Pop();
                  DrawSheet(viewModel, nextPage, currentFold, viewPort);
               }
               else
               {
                  DrawSheet(viewModel, currentSheet, currentFold, viewPort);
               }
               break;
            case Input.PrevSheet:
               if (readSheets.Any())
               {
                  unreadSheets.Push(currentSheet);
                  var lastPage = readSheets.Pop();
                  DrawSheet(viewModel, lastPage, currentFold, viewPort);
               }
               else
               {
                  DrawSheet(viewModel, currentSheet, currentFold, viewPort);
               }
               break;
            case Input.NextFold:
               if (unreadFolds.Any())
               {
                  readFolds.Push(currentFold);
                  var nextFold = unreadFolds.Pop();
                  DrawSheet(viewModel, currentSheet, nextFold, viewPort);
               }
               else
               {
                  DrawSheet(viewModel, currentSheet, currentFold, viewPort);
               }
               break;
            case Input.PrevFold:
               if (readFolds.Any())
               {
                  unreadFolds.Push(currentFold);
                  var lastFold = readFolds.Pop();
                  DrawSheet(viewModel, currentSheet, lastFold, viewPort);
               }
               else
               {
                  DrawSheet(viewModel, currentSheet, currentFold, viewPort);
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
            return Input.NextSheet;
         }
         else if (key.Key == ConsoleKey.LeftArrow)
         {
            return Input.PrevSheet;
         }
         else if (key.Key == ConsoleKey.Enter)
         {
            return Input.Quit;
         }
         else if (key.Key == ConsoleKey.DownArrow)
         {
            return Input.NextFold;
         }
         else if (key.Key == ConsoleKey.UpArrow)
         {
            return Input.PrevFold;
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

      private void WriteSummary(ViewModel viewModel, ConsoleSheet currentSheet, ConsoleFold currentFold)
      {
         Console.WriteLine("Showing {0} to {1} of {2} Columns. Use Arrow Keys to Navigate.", currentSheet.IndexStart, currentSheet.IndexEnd, viewModel.Columns.Count());
         Console.WriteLine("Showing {0} to {1} of {2} Rows Total. Press ENTER to quit;", currentFold.IndexStart, currentFold.IndexEnd, viewModel.RowCount);
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

      private void WriteValues(ViewModel viewModel, ConsoleSheet columnsFitToScreen, ConsoleFold foldedRows)
      {
         for (int i = 0; i < foldedRows.Rows.Count(); i++)
         {
            var row = foldedRows.Rows.ElementAt(i);
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
