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
   public class FullConsoleView : IDrawViews
   {
      private const string verticalSeparator = "|";
      private const string horizontalSeparator = "-";
      private const string cellDivider = "+";

      public void Draw(ViewModel viewModel, ViewSettings settings)
      {
         Console.Clear();

         DrawSheet(viewModel, settings.displayNulls);

      }

      private void DrawSheet(ViewModel viewModel, bool displayNulls)
      {
         Console.Clear();
         DrawLine(viewModel.Columns);
         WriteHeaderLine(viewModel);
         DrawLine(viewModel.Columns);
         WriteValues(viewModel, displayNulls);
         DrawLine(viewModel.Columns);
         WriteSummary(viewModel);

      }

      private void WriteSummary(ViewModel viewModel)
      {
         Console.WriteLine("Showing {0} Columns with {1} Rows.", viewModel.Columns.Count(), viewModel.RowCount);
      }
      private void WriteHeaderLine(ViewModel columnDetails)
      {
         Console.Write(verticalSeparator);
         foreach (ColumnDetails column in columnDetails.Columns)
         {
            for (int i = 0; i < column.columnWidth - column.columnName.Length; i++)
            {
               Console.Write(" ");
            }

            Console.Write(column.columnName);
            Console.Write(verticalSeparator);
         }
         Console.Write(Environment.NewLine);
      }

      private void WriteValues(ViewModel viewModel, bool displayNulls)
      {
         for (int i = 0; i < viewModel.Rows.Count(); i++)
         {
            object[] row = viewModel.Rows.ElementAt(i);
            Console.Write(verticalSeparator);
            for (int j = 0; j < row.Length; j++)
            {
               ColumnDetails header = viewModel.Columns.ElementAt(j);

               Console.Write(header.GetFormattedValue(row[j], new ViewPort(), displayNulls, verticalSeparator));
               Console.Write(verticalSeparator);
            }
            Console.WriteLine();
         }
      }
      private void DrawLine(IEnumerable<ColumnDetails> columns)
      {
         Console.Write(cellDivider);
         foreach (int item in columns.Select(d => d.columnWidth))
         {
            for (int i = 0; i < item; i++)
            {
               Console.Write(horizontalSeparator);
            }
            Console.Write(cellDivider);
         }
         Console.Write(Environment.NewLine);
      }
   }
}
