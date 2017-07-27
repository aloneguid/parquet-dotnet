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
      private ViewPort viewPort;
      private ConsoleSheet _currentSheet;
      private ConsoleFold _currentFold;

      internal event EventHandler<ConsoleFold> FoldRequested;

      public InteractiveConsoleView()
      {
          viewPort = new ViewPort();
      }

      public void Draw(ViewModel viewModel)
      { 
         Console.Clear();

         unreadSheets = GenerateSheets(viewPort, viewModel.Columns);
         unreadFolds = GenerateFolds(viewPort, viewModel.RowCount);

         _currentSheet = unreadSheets.Pop();
         _currentFold = unreadFolds.Pop();
         DrawSheet(viewModel, _currentSheet, _currentFold, viewPort);
         
      }

      public void Update(ViewModel viewModel)
      {
         Console.Clear();
         DrawSheet(viewModel, _currentSheet, _currentFold, viewPort);
      }

      private Stack<ConsoleFold> GenerateFolds(ViewPort viewPort, long totalRowCount)
      {
         var foldLength = GetRowCount();

         var folds = new Stack<ConsoleFold>();
         var firstFoldPoint = (totalRowCount > foldLength) ? foldLength : totalRowCount;
         var runningTotal = firstFoldPoint;

         folds.Push(new ConsoleFold { IndexStart = 0, IndexEnd = firstFoldPoint });

         while (runningTotal != totalRowCount)
         {
            var nextFoldPoint = (totalRowCount > runningTotal + foldLength) ? foldLength : totalRowCount - runningTotal;
            folds.Push(new ConsoleFold { IndexStart = runningTotal, IndexEnd = runningTotal + nextFoldPoint });
            runningTotal += nextFoldPoint;
         }

         var originalFoldCount = folds.Count;
         var unReadFolds = new Stack<ConsoleFold>();
         for (int i = 0; i < originalFoldCount; i++)
         {
            unReadFolds.Push(folds.Pop());
         }
         return unReadFolds;
      }

      internal int GetRowCount()
      {
         return viewPort.Height - 7;
      }

      private void DrawSheet(ViewModel viewModel, ConsoleSheet currentSheet, ConsoleFold currentFold,  ViewPort viewPort)
      {
         Console.Clear();
         DrawLine(currentSheet, viewPort);
         WriteHeaderLine(currentSheet, viewPort);
         DrawLine(currentSheet, viewPort);
         WriteValues(viewModel, currentSheet, currentFold, viewPort);
         DrawLine(currentSheet, viewPort);
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
                  _currentFold = nextFold;

                  this.FoldRequested?.Invoke(this, nextFold);
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
                  _currentFold = lastFold;

                  this.FoldRequested?.Invoke(this, lastFold);
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
                  if (!IsSingleOverlyLargeColumn(chosenColumns, column, viewPort))
                  {
                     return chosenColumns;
                  }
               }
               runningTotal += column.columnWidth + verticalSeparator.Length;
               chosenColumns.Add(column);
            }
         }

         return columns;
      }

      private bool IsSingleOverlyLargeColumn(List<ColumnDetails> chosenColumns, ColumnDetails column, ViewPort viewPort)
      {
         return !chosenColumns.Any() && column.columnWidth + verticalSeparator.Length > viewPort.Width;
      }

      private bool IsOverlyLargeColumn(ColumnDetails column, ViewPort viewPort)
      {
         return column.columnWidth + verticalSeparator.Length > viewPort.Width;
      }

      private void WriteSummary(ViewModel viewModel, ConsoleSheet currentSheet, ConsoleFold currentFold)
      {
         Console.WriteLine("Showing {0} to {1} of {2} Columns. Use Arrow Keys to Navigate.", currentSheet.IndexStart, currentSheet.IndexEnd, viewModel.Columns.Count());
         Console.WriteLine("Showing {0} to {1} of {2} Rows Total. Press ENTER to quit;", currentFold.IndexStart, currentFold.IndexEnd, viewModel.RowCount);
      }
      private void WriteHeaderLine(ConsoleSheet sheet, ViewPort viewPort)
      {
         Console.Write(verticalSeparator);
         foreach (var column in sheet.Columns)
         {
            if (IsOverlyLargeColumn(column, viewPort))
            {
               for (int i = 0; i < viewPort.Width - column.columnName.Length - (verticalSeparator.Length*2) - Environment.NewLine.Length; i++)
               {
                  Console.Write(" ");
               }
            }
            else
            {
               for (int i = 0; i < column.columnWidth - column.columnName.Length; i++)
               {
                  Console.Write(" ");
               }
            }

            Console.Write(column.columnName);
            Console.Write(verticalSeparator);
         }
         Console.Write(Environment.NewLine);
      }

      private void WriteValues(ViewModel viewModel, ConsoleSheet columnsFitToScreen, ConsoleFold foldedRows, ViewPort viewPort)
      {
         for (int i = 0; i < viewModel.Rows.Count(); i++)
         {
            var row = viewModel.Rows.ElementAt(i);
            Console.Write(verticalSeparator);
            for (int j = 0; j < row.Length; j++)
            {
               var header = viewModel.Columns.ElementAt(j);
               if (columnsFitToScreen.Columns.Any(x => x.columnName == header.columnName))
               {
                  var persistedFit = columnsFitToScreen.Columns.First(x => x.columnName == header.columnName);
                  var data = persistedFit.GetFormattedValue(row[j]);

                  if (IsOverlyLargeColumn(persistedFit, viewPort))
                  {
                     if (data.Length > viewPort.Width)
                     {
                        Console.Write(data.Substring(0, viewPort.Width - (verticalSeparator.Length*2) - Environment.NewLine.Length - AppSettings.Instance.TruncationIdentifier.Value.Length));

                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.BackgroundColor = ConsoleColor.Black;
                        Console.Write(AppSettings.Instance.TruncationIdentifier);
                        Console.ResetColor();
                     }
                  }
                  else if (data.Contains("[null]"))
                  {
                     Console.ForegroundColor = ConsoleColor.DarkGray;
                     Console.Write(data);
                     Console.ResetColor();
                  }
                  else
                  {
                     Console.Write(data);
                  }

                  Console.Write(verticalSeparator);
               }
            }
            Console.WriteLine();
         }
      }
      private void DrawLine(ConsoleSheet consoleSheet, ViewPort viewPort)
      {
         Console.Write(verticalSeparator);
         foreach (var column in consoleSheet.Columns)
         {
            if (IsOverlyLargeColumn(column, viewPort))
            {
               for (int i = 0; i < viewPort.Width - (verticalSeparator.Length*2) - Environment.NewLine.Length; i++)
               {
                  Console.Write(horizontalSeparator);
               }
            }
            else
            {
               for (int i = 0; i < column.columnWidth; i++)
               {
                  Console.Write(horizontalSeparator);
               }
            }
            Console.Write(verticalSeparator);
         }
         Console.Write(Environment.NewLine);
      }
   }
}
