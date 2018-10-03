using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cpf.App;
using Parquet.CLI.Models;
using Parquet.CLI.Views;
using Parquet.Data;
using Parquet.Data.Rows;

namespace Parquet.CLI.Commands
{
    class DisplayFullCommand<TViewType> : FileInputCommand
                              where TViewType : IDrawViews, new()
    {
      public DisplayFullCommand(string path) : base(path)
      {
      }

      public ViewModel Get(Table dataSet, bool expandCells, int displayMinWidth)
      {
         var viewModel = new ViewModel();
         IEnumerable<ColumnDetails> columns = GenerateColumnList(dataSet, expandCells, displayMinWidth);
         viewModel.Columns = columns;
         viewModel.Rows = new List<object[]>();

         for (int i = 0; i < dataSet.Count; i++)
         {
            Row row = dataSet[i];
            viewModel.Rows.Add(row.Values);
         }
         viewModel.RowCount = dataSet.Count;
         viewModel.Schema = dataSet.Schema;
         return viewModel;
      }
      private IEnumerable<ColumnDetails> GenerateColumnList(Table dataSet, bool expanded, int displayMinWidth)
      {
         IEnumerable<ColumnDetails> columnDetails = dataSet.Schema.Fields.OfType<DataField>().Select(column =>
            new ColumnDetails {
               columnWidth = column.Name.Length,
               columnName = column.Name,
               type = column.DataType,
               isNullable = true
            });
         var parsedSet = new List<ColumnDetails>();
         for (int i = 0; i < columnDetails.Count(); i++)
         {
            ColumnDetails column = columnDetails.ElementAt(i);

            if (expanded)
            {
               column.columnWidth = dataSet.Max(row =>
               {
                  string rawVal = row.Values[i]?.ToString();
                  if (rawVal == null)
                     return column.columnName.Length;

                  string rawStr = rawVal.ToString();
                  if (rawStr.Length > column.columnName.Length)
                     return rawStr.Length;

                  return column.columnName.Length;
               });
            }

            if (column.columnWidth < displayMinWidth)
            {
               column.columnWidth = displayMinWidth;
            }
            parsedSet.Add(column);
         }
         return parsedSet;
      }

      internal void Execute(ViewSettings settings)
      {
         Table table = ReadTable();
         ViewModel viewModel = Get(table, settings.expandCells, settings.displayMinWidth);
         new TViewType().Draw(viewModel, settings);
      }
   }
}
