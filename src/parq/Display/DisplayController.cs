using Parquet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace parq.Display
{
    internal class DisplayController
    {
      public ViewModel Get(ParquetDataSet dataSet)
      {
         var viewModel = new ViewModel();
         var columns = GenerateColumnList(dataSet);
         viewModel.Columns = columns;
         viewModel.Rows = new List<object[]>();

         for (int i = 0; i < dataSet.Count; i++)
         {
            var row = dataSet[i];
            viewModel.Rows.Add(row);
         }
         viewModel.RowCount = dataSet.Count;
         return viewModel;
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

   }
}
