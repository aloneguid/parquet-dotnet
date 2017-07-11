using parq.Display.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace parq.Display.Views
{
   /// <summary>
   /// Displays to the Console window a navigatable (2D) data set
   /// </summary>
   public class FullConsoleView
   {
      private const string verticalSeparator = "|";
      private const string horizontalSeparator = "-";

      public void Draw(ViewModel viewModel)
      {
         Console.Clear();

         DrawSheet(viewModel);

      }

      private void DrawSheet(ViewModel viewModel)
      {
         Console.Clear();
         DrawLine(viewModel.Columns);
         WriteHeaderLine(viewModel.Columns);
         DrawLine(viewModel.Columns);
         WriteValues(viewModel);
         DrawLine(viewModel.Columns);
         WriteSummary(viewModel);

      }

      private void WriteSummary(ViewModel viewModel)
      {
         Console.WriteLine("Showing {0} Columns with {1} Rows.", viewModel.Columns.Count(), viewModel.RowCount);
      }
      private void WriteHeaderLine(IEnumerable<ColumnDetails> columnDetails)
      {
         Console.Write(verticalSeparator);
         foreach (string name in columnDetails.Select(cd => cd.columnName))
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

      private void WriteValues(ViewModel viewModel)
      {
         for (int i = 0; i < viewModel.Rows.Count(); i++)
         {
            var row = viewModel.Rows.ElementAt(i);
            Console.Write(verticalSeparator);
            for (int j = 0; j < row.Length; j++)
            {
               var header = viewModel.Columns.ElementAt(j);

               Console.Write(header.GetFormattedValue(row[j]));
               Console.Write(verticalSeparator);
            }
            Console.WriteLine();
         }
      }
      private void DrawLine(IEnumerable<ColumnDetails> columns)
      {
         Console.Write(verticalSeparator);
         foreach (int item in columns.Select(d => d.columnWidth))
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
