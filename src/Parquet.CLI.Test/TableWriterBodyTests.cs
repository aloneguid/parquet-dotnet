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
   public class TableWriterBodyTests
   {
      [Fact]
      public void TableWriterWritesSomething()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable() {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } } ,
            ColumnDetails = new [] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String  } },
            Rows = new [] { new TableRow { Cells = new [] { new BasicTableCell {  ContentAreas = new [] { new BasicCellContent { Value = "This is a test value."  } }  } } } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.True(!string.IsNullOrEmpty(s));
      }

      [Fact]
      public void TableWriterWritesExpected()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));
         Guid expectedValue = Guid.NewGuid();

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = expectedValue.ToString().Length+2, isNullable = false, type = Data.DataType.String } },
            Rows = new[] { new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = expectedValue.ToString() } } } } } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.Contains(expectedValue.ToString(), s);
      }


      [Fact]
      public void TableWriterWritesExpectedTwoColumns()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));
         Guid expectedValue1 = Guid.NewGuid();
         Guid expectedValue2 = Guid.NewGuid();


         var table = new DisplayTable()
         {
            Header = new TableRow
            {
               Cells = new[] {
               new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell1" } } },
               new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell2" } } },
            }
            },
            ColumnDetails = new[] {
               new ColumnDetails { columnName = "harrumph", columnWidth = expectedValue1.ToString().Length+2, isNullable = false, type = Data.DataType.String },
               new ColumnDetails { columnName = "harrumphflup", columnWidth = expectedValue2.ToString().Length+2, isNullable = false, type = Data.DataType.String },
            },
            Rows = new[] { new TableRow { Cells = new[] {
               new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = expectedValue1.ToString() } } },
               new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = expectedValue2.ToString() } } }
            } } }

         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.Contains(expectedValue1.ToString(), s);
         Assert.Contains(expectedValue2.ToString(), s);
      }
   }
}
