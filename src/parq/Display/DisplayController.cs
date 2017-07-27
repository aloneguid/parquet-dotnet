using parq.Display.Models;
using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace parq.Display
{
    internal class DisplayController
    {
      public ViewModel Get(DataSet dataSet)
      {
         var viewModel = new ViewModel();
         var columns = GenerateColumnList(dataSet);
         viewModel.Columns = columns;
         viewModel.Rows = new List<object[]>();

         for (int i = 0; i < dataSet.Count; i++)
         {
            Row row = dataSet[i];
            viewModel.Rows.Add(row.RawValues);
         }
         viewModel.RowCount = dataSet.TotalRowCount;
         viewModel.Schema = dataSet.Schema;
         return viewModel;
      }
      private IEnumerable<ColumnDetails> GenerateColumnList(DataSet dataSet)
      {
         var columnDetails = dataSet.Schema.Elements.Select(column =>
            new ColumnDetails { columnWidth = column.Name.Length, columnName = column.Name });
         var parsedSet = new List<ColumnDetails>();
         for (var i = 0; i<columnDetails.Count(); i++)
         {
            var column = columnDetails.ElementAt(i);

            if (AppSettings.Instance.Expanded)
            {
               column.columnWidth = dataSet.Max(row => {
                  var rawVal = row.RawValues.ElementAt(i);
                  if (rawVal == null)
                     return column.columnName.Length;

                  var rawStr = rawVal.ToString();
                  if (rawStr.Length > column.columnName.Length)
                     return rawStr.Length;

                  return column.columnName.Length;
                  });
            }

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
