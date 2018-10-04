using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.CLI.Models;

namespace Parquet.CLI.Views
{
   /// <summary>
   /// Displays to the Console window a navigatable (2D) data set
   /// </summary>
   public class InteractiveConsoleView : IDrawViews
   {
      private const string verticalSeparator = "|";
      private const string horizontalSeparator = "-";
      private const string cellDivider = "+";
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

      public void Draw(ViewModel viewModel, ViewSettings settings)
      {
         Console.Clear();

         unreadSheets = GenerateSheets(viewPort, viewModel.Columns, settings.displayMinWidth);
         unreadFolds = GenerateFolds(viewPort, (int)viewModel.RowCount);

         _currentSheet = unreadSheets.Pop();
         _currentFold = unreadFolds.Pop();
         DrawSheet(viewModel, _currentSheet, _currentFold, viewPort, settings.displayTypes, settings.displayNulls, settings.truncationIdentifier, settings.displayReferences);

      }

      public void Update(ViewModel viewModel, bool displayTypes, bool displayNulls, string truncationIdentifier, bool displayRefs)
      {
         Console.Clear();
         DrawSheet(viewModel, _currentSheet, _currentFold, viewPort, displayTypes, displayNulls, truncationIdentifier, displayRefs);
      }

      private Stack<ConsoleFold> GenerateFolds(ViewPort viewPort, int totalRowCount)
      {
         int foldLength = GetRowCount();

         Stack<ConsoleFold> folds = new Stack<ConsoleFold>();
         int firstFoldPoint = (totalRowCount > foldLength) ? foldLength : totalRowCount;
         int runningTotal = firstFoldPoint;

         folds.Push(new ConsoleFold { IndexStart = 0, IndexEnd = firstFoldPoint });

         while (runningTotal != totalRowCount)
         {
            int nextFoldPoint = (totalRowCount > runningTotal + foldLength) ? foldLength : totalRowCount - runningTotal;
            folds.Push(new ConsoleFold { IndexStart = runningTotal, IndexEnd = runningTotal + nextFoldPoint });
            runningTotal += nextFoldPoint;
         }

         int originalFoldCount = folds.Count;
         var unReadFolds = new Stack<ConsoleFold>();
         for (int i = 0; i < originalFoldCount; i++)
         {
            unReadFolds.Push(folds.Pop());
         }
         return unReadFolds;
      }

      internal int GetRowCount()
      {
         return viewPort.Height - 8;
      }

      private void DrawSheet(ViewModel viewModel, ConsoleSheet currentSheet, ConsoleFold currentFold, ViewPort viewPort, bool displayTypes, bool displayNulls, string truncationIdentifier, bool displayRefs)
      {
         Console.Clear();
         DrawLine(currentSheet, viewPort, displayRefs);
         WriteHeaderLine(currentSheet, viewPort, displayTypes);
         DrawLine(currentSheet, viewPort);
         WriteValues(viewModel, currentSheet, currentFold, viewPort, displayNulls, truncationIdentifier, displayRefs);
         DrawLine(currentSheet, viewPort);
         WriteSummary(viewModel, currentSheet, currentFold, displayNulls);

         Input input = AwaitInput();
         switch (input)
         {
            case Input.NoOp:
               DrawSheet(viewModel, currentSheet, currentFold, viewPort, displayTypes, displayNulls, truncationIdentifier, displayRefs);
               break;
            case Input.Quit:
               break;
            case Input.NextSheet:
               if (unreadSheets.Any())
               {
                  readSheets.Push(currentSheet);
                  ConsoleSheet nextPage = unreadSheets.Pop();
                  DrawSheet(viewModel, nextPage, currentFold, viewPort, displayTypes, displayNulls, truncationIdentifier, displayRefs);
               }
               else
               {
                  DrawSheet(viewModel, currentSheet, currentFold, viewPort, displayTypes, displayNulls, truncationIdentifier, displayRefs);
               }
               break;
            case Input.PrevSheet:
               if (readSheets.Any())
               {
                  unreadSheets.Push(currentSheet);
                  ConsoleSheet lastPage = readSheets.Pop();
                  DrawSheet(viewModel, lastPage, currentFold, viewPort, displayTypes, displayNulls, truncationIdentifier, displayRefs);
               }
               else
               {
                  DrawSheet(viewModel, currentSheet, currentFold, viewPort, displayTypes, displayNulls, truncationIdentifier, displayRefs);
               }
               break;
            case Input.NextFold:
               if (unreadFolds.Any())
               {
                  readFolds.Push(currentFold);
                  ConsoleFold nextFold = unreadFolds.Pop();
                  _currentFold = nextFold;

                  this.FoldRequested?.Invoke(this, nextFold);
               }
               else
               {
                  DrawSheet(viewModel, currentSheet, currentFold, viewPort, displayTypes, displayNulls, truncationIdentifier, displayRefs);
               }
               break;
            case Input.PrevFold:
               if (readFolds.Any())
               {
                  unreadFolds.Push(currentFold);
                  ConsoleFold lastFold = readFolds.Pop();
                  _currentFold = lastFold;

                  this.FoldRequested?.Invoke(this, lastFold);
               }
               else
               {
                  DrawSheet(viewModel, currentSheet, currentFold, viewPort, displayTypes, displayNulls, truncationIdentifier, displayRefs);
               }
               break;
         }
      }

      private Stack<ConsoleSheet> GenerateSheets(ViewPort viewPort, IEnumerable<ColumnDetails> columns, int displayMinWidth)
      {
         var pages = new Stack<ConsoleSheet>();
         IEnumerable<ColumnDetails> firstPage = FitColumns(viewPort, columns, displayMinWidth);
         var sheet = new ConsoleSheet { Columns = firstPage, IndexStart = 0, IndexEnd = firstPage.Count() };
         pages.Push(sheet);

         int runningTotal = firstPage.Count();
         while (runningTotal != columns.Count())
         {
            IEnumerable<ColumnDetails> nextPage = FitColumns(viewPort, columns.Skip(runningTotal), displayMinWidth);
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
         ConsoleKeyInfo key = Console.ReadKey();
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
         //else if (LookupCols.Contains(key.Key))
         //{
         //first draw refs
         //}

         return Input.NoOp;
      }

      private IEnumerable<ColumnDetails> FitColumns(ViewPort viewPort, IEnumerable<ColumnDetails> columns, int displayMinWidth)
      {
         int allColumnsWidth = columns
            .Select(col => (col.columnWidth < displayMinWidth ? displayMinWidth : col.columnWidth) + verticalSeparator.Length)
            .Sum() + verticalSeparator.Length;

         if (allColumnsWidth > viewPort.Width)
         {
            int runningTotal = 0;
            List<ColumnDetails> chosenColumns = new List<ColumnDetails>();
            foreach (ColumnDetails column in columns)
            {
               if (runningTotal + column.columnWidth + verticalSeparator.Length > viewPort.Width)
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

      private void WriteSummary(ViewModel viewModel, ConsoleSheet currentSheet, ConsoleFold currentFold, bool displayNulls)
      {
         Console.Write("Showing {0} to {1} of {2} Columns. Use Arrow Keys to Navigate. ", currentSheet.IndexStart, currentSheet.IndexEnd, viewModel.Columns.Count());

         if (displayNulls)
         {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.Write(".NET type name. ");
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.Write("? denotes Nullable=true.{0}", Environment.NewLine);
            Console.ResetColor();
         }
         Console.WriteLine("Showing {0} to {1} of {2} Rows Total. Press ENTER to quit;", currentFold.IndexStart, currentFold.IndexEnd, viewModel.RowCount);
      }
      private void WriteHeaderLine(ConsoleSheet sheet, ViewPort viewPort, bool displayTypes)
      {
         Console.Write(verticalSeparator);
         foreach (ColumnDetails column in sheet.Columns)
         {
            if (IsOverlyLargeColumn(column, viewPort))
            {
               for (int i = 0; i < viewPort.Width - column.columnName.Length - (verticalSeparator.Length * 2) - Environment.NewLine.Length; i++)
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
         if (displayTypes)
         {
            Console.Write(verticalSeparator);
            foreach (ColumnDetails column in sheet.Columns)
            {
               int offset = column.isNullable ? 1 : 0;
               if (IsOverlyLargeColumn(column, viewPort))
               {
                  for (int i = 0; i < viewPort.Width - offset - column.type.ToString().Length - (verticalSeparator.Length * 2) - Environment.NewLine.Length; i++)
                  {
                     Console.Write(" ");
                  }
               }
               else
               {
                  for (int i = 0; i < column.columnWidth - column.type.ToString().Length - offset; i++)
                  {
                     Console.Write(" ");
                  }
               }
               Console.ForegroundColor = ConsoleColor.Blue;

               if (column.columnWidth > column.type.ToString().Length + (verticalSeparator.Length * 2))
               {
                  Console.Write(column.type.ToString());
               }
               else
               {
                  int howMuchSpaceDoWeHave = column.isNullable ? column.columnWidth - 1 : column.columnWidth;
                  Console.Write(column.type.ToString().Substring(0, howMuchSpaceDoWeHave));
               }


               if (column.isNullable)
               {
                  Console.ForegroundColor = ConsoleColor.Cyan;
                  Console.Write("?");
                  Console.ResetColor();
               }
               Console.Write(verticalSeparator);
            }
            Console.Write(Environment.NewLine);
         }
      }

      private void WriteValues(ViewModel viewModel, ConsoleSheet columnsFitToScreen, ConsoleFold foldedRows, ViewPort viewPort, bool displayNulls, string truncationIdentifier, bool displayRefs)
      {
         for (int i = 0; i < viewModel.Rows.Count(); i++)
         {
            object[] row = viewModel.Rows.ElementAt(i);
            Console.Write(verticalSeparator);
            for (int j = 0; j < row.Length; j++)
            {
               ColumnDetails header = viewModel.Columns.ElementAt(j);
               if (columnsFitToScreen.Columns.Any(x => x.columnName == header.columnName))
               {
                  ColumnDetails persistedFit = columnsFitToScreen.Columns.First(x => x.columnName == header.columnName);
                  string data = persistedFit.GetFormattedValue(row[j], displayNulls);

                  if (IsOverlyLargeColumn(persistedFit, viewPort))
                  {
                     if (data.Length > viewPort.Width)
                     {
                        Console.Write(data.Substring(0, viewPort.Width - (verticalSeparator.Length * 2) - Environment.NewLine.Length - truncationIdentifier.Length));

                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.BackgroundColor = ConsoleColor.Black;
                        Console.Write(truncationIdentifier);
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
      private void DrawLine(ConsoleSheet consoleSheet, ViewPort viewPort, bool drawRefs = false)
      {
         Console.Write(cellDivider);
         for (int c = 0; c < consoleSheet.Columns.Count(); c++)
         {
            ColumnDetails column = consoleSheet.Columns.ElementAt(c);

            int columnNameLength = c.ToString().Length;
            if (IsOverlyLargeColumn(column, viewPort))
            {
               for (int i = 0; i < ((viewPort.Width - (verticalSeparator.Length * 2) - Environment.NewLine.Length) / 2) - (columnNameLength / 2); i++)
               {
                  Console.Write(horizontalSeparator);
               }
               if (drawRefs)
               {
                  Console.Write(c.ToString());
               }
               else
               {
                  Console.Write(horizontalSeparator);
               }
               for (int i = 0; i < ((viewPort.Width - (verticalSeparator.Length * 2) - Environment.NewLine.Length) / 2) - (columnNameLength / 2) - ((columnNameLength + 1) % 2); i++)
               {
                  Console.Write(horizontalSeparator);
               }
            }
            else
            {
               for (int i = 0; i < (column.columnWidth / 2) - (c.ToString().Length / 2); i++)
               {
                  Console.Write(horizontalSeparator);
               }
               if (drawRefs)
               {
                  Console.Write(c.ToString());
               }
               else
               {
                  Console.Write(horizontalSeparator);
               }
               for (int i = 0; i < (column.columnWidth / 2) - (c.ToString().Length / 2) - ((column.columnWidth + 1) % 2); i++)
               {
                  Console.Write(horizontalSeparator);
               }
            }
            Console.Write(cellDivider);
         }
         Console.Write(Environment.NewLine);
      }
   }
}