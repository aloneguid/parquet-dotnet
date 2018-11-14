using System;
using System.Linq;
using Moq;
using Parquet.CLI.Models;
using Parquet.CLI.Models.Tabular;
using Parquet.CLI.Test.Helper;
using Parquet.CLI.Views.Tablular;
using Xunit;

namespace Parquet.CLI.Test
{
   public class TableWriterMultirowHeadersTests
   {
      [Fact]
      public void TableWriterWritesSomething()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable() {
            Header = new TableRow { Cells = new[] { new BasicTableCell {
               ContentAreas = new ICellContent[] {
                  new BasicCellContent { Value = "TestCell" },
                  new BreakingRuleContentArea(),
                  new BasicCellContent { Value = "Line2" }
               }
            } } } ,
            ColumnDetails = new [] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String  } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.Contains("Line2", s);
      }

      [Fact]
      public void TableWriterMultipleRows()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable()
         {
            Header = new TableRow
            {
               Cells = new[] { new BasicTableCell {
               ContentAreas = new ICellContent[] {
                  new BasicCellContent { Value = "TestCell" },
                  new BreakingRuleContentArea(),
                  new BasicCellContent { Value = "Line2" }
               }
            } }
            },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);

         string[] effectiveLines = s.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
         Assert.Equal(4, effectiveLines.Length);
      }

   }
}
