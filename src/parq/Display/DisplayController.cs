using Parquet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace parq.Display
{
    internal class DisplayController
    {
      public void Draw(ParquetDataSet dataSet)
      {
         Console.Clear();
         var viewPort = new ViewPort();

         var chosenHeaders = GenerateColumnList(dataSet);
         DrawLine(chosenHeaders);
         WriteHeaderLine(chosenHeaders);
         DrawLine(chosenHeaders);
         WriteValues(dataSet, chosenHeaders);
         DrawLine(chosenHeaders);
         WriteSummary(dataSet);
      }

      private void WriteSummary(ParquetDataSet dataSet)
      {
         Console.WriteLine("{0} Rows Affected. Press ENTER to quit;", dataSet.Count);
      }

      private void WriteValues(ParquetDataSet dataSet, IEnumerable<ColumnDetails> chosenHeaders)
      {
         for (int i = 0; i < dataSet.Count; i++)
         {
            var row = dataSet[i];
            Console.Write("|");
            for (int j = 0; j < row.Length; j++)
            {
               var value = Convert.ToString(row[j]);
               var header = chosenHeaders.ElementAt(j);
               var viewModel = new ViewModel { Column = header, RawValue = value };
               Console.Write(viewModel.GetFormattedValue());
               Console.Write("|");
            }
            Console.WriteLine();
         }
      }

      private IEnumerable<ColumnDetails> GenerateColumnList(ParquetDataSet dataSet)
      {
         var columnDetails = dataSet.ColumnNames.Select(column => 
            new ColumnDetails { columnWidth = column.Length, columnName = column });
         var parsedSet = new List<ColumnDetails>();
         foreach (var column in columnDetails)
         {
            if (column.columnWidth < AppSettings.Instance.DisplayMinWidth.Value)
            {
               column.columnWidth = AppSettings.Instance.DisplayMinWidth.Value;
            }
            parsedSet.Add(column);
         }
         return parsedSet;
      }

      private void WriteHeaderLine(IEnumerable<ColumnDetails> columnDetails)
      {
         Console.Write("|");
         foreach (string name in columnDetails.Select(cd=>cd.columnName))
         {
            if (name.Length < AppSettings.Instance.DisplayMinWidth.Value)
            {
               for (int i = 0; i < AppSettings.Instance.DisplayMinWidth.Value - name.Length; i++)
               {
                  Console.Write(" ");
               }
            }
            Console.Write(name);
            Console.Write("|");
         }
         Console.Write(Environment.NewLine);
      }

      private void DrawLine(IEnumerable<ColumnDetails> columnHeaderSizes)
      {
         Console.Write("|");
         foreach (int item in columnHeaderSizes.Select(d => d.columnWidth))
         {
            for (int i = 0; i < item; i++)
            {
               Console.Write("-");
            }
            Console.Write("|");
         }
         Console.Write(Environment.NewLine);
      }
   }
}
