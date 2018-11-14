using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.CLI.Models;
using Parquet.CLI.Models.Tabular;

namespace Parquet.CLI.Views
{
   /// <summary>
   /// Displays to the Console window a navigatable (2D) data set
   /// </summary>
   public class InteractiveConsoleView : IDrawViews
   {
      private Stack<ConsoleSheet> unreadSheets;
      private Stack<ConsoleSheet> readSheets = new Stack<ConsoleSheet>();
      private Stack<ConsoleFold> unreadFolds;
      private Stack<ConsoleFold> readFolds = new Stack<ConsoleFold>();
      private ViewPort viewPort;
      private ConsoleSheet _currentSheet;
      private ConsoleFold _currentFold;
      private ViewSettings _settings;

      public InteractiveConsoleView()
      {
         viewPort = new ViewPort();
      }

      public void Draw(ViewModel viewModel, ViewSettings settings)
      {
         _settings = settings;
         Console.Clear();

         unreadSheets = GenerateSheets(viewPort, viewModel.Columns, settings.displayMinWidth);
         unreadFolds = GenerateFolds(viewPort, (int)viewModel.RowCount);

         _currentSheet = unreadSheets.Pop();
         _currentFold = unreadFolds.Pop();
         DrawSheet(viewModel, _currentSheet, _currentFold, viewPort, settings.displayTypes, settings.displayNulls, settings.truncationIdentifier, settings.displayReferences);
         Console.Clear();
         Console.WriteLine("Exiting...");
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
         DisplayTable displayTable = new DisplayTable();
         displayTable.Header = GenerateHeaderFromSheet(viewModel, currentSheet);
         displayTable.ColumnDetails = currentSheet.Columns.ToArray();
         displayTable.Rows = GenerateRowsFromFold(viewModel, currentFold, currentSheet);
         new Tablular.TableWriter(viewPort).Draw(displayTable);

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
                  _currentSheet = nextPage;

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
                  _currentSheet = lastPage;

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

                  this.Update(viewModel, displayTypes, displayNulls, truncationIdentifier, displayRefs);
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

                  this.Update(viewModel, displayTypes, displayNulls, truncationIdentifier, displayRefs);
               }
               else
               {
                  DrawSheet(viewModel, currentSheet, currentFold, viewPort, displayTypes, displayNulls, truncationIdentifier, displayRefs);
               }
               break;
         }
      }

      private TableRow[] GenerateRowsFromFold(ViewModel viewModel, ConsoleFold currentFold, ConsoleSheet currentSheet)
      {
         List<TableRow> rows = new List<TableRow>();
         for (int i = currentFold.IndexStart; i < currentFold.IndexEnd; i++)
         {
            var row = new TableRow();
            List<TableCell> cells = new List<TableCell>();
            for (int j = currentSheet.IndexStart; j < currentSheet.IndexEnd; j++)
            {
               cells.Add(new BasicTableCell
               {
                  
                  ContentAreas = new[] { new BasicCellContent
                     {
                        Value = viewModel.Rows[i][j]
                     }
                  }
               });
            }
            row.Cells = cells.ToArray();
            rows.Add(row);
         }
         return rows.ToArray();
      }

      private TableRow GenerateHeaderFromSheet(ViewModel viewModel, ConsoleSheet currentSheet)
      {
         var row = new TableRow();
         List<TableCell> headers = new List<TableCell>();
         for (int i = currentSheet.IndexStart; i < currentSheet.IndexEnd; i++)
         {
            ColumnDetails column = currentSheet.Columns.ElementAt(i - currentSheet.IndexStart);

            var content = new List<ICellContent>();
            content.Add(
                  new BasicCellContent
                  {
                     Value = column.columnName
                  });
            if (_settings.displayTypes)
            {
               content.Add(new BreakingRuleContentArea());
               content.Add(new BasicCellContent { ForegroundColor = ConsoleColor.Blue, Value = column.type.ToString() });
            }
            headers.Add(new BasicTableCell
            {
               
               ContentAreas = content.ToArray()
            });
         }
         row.Cells = headers.ToArray();
         return row;
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

      // todo: retire the following
      private const string verticalSeparator = "|";
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
               if (runningTotal + column.columnWidth + verticalSeparator.Length >= viewPort.Width)
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
      
   }
}