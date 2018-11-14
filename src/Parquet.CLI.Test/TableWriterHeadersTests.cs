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
   public class TableWriterHeadersTests
   {
      [Fact]
      public void TableWriterWritesSomething()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable() {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } } ,
            ColumnDetails = new [] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String  } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.True(!string.IsNullOrEmpty(s));
      }

      [Fact]
      public void TableWriterWritesHeader()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.Contains("TestCell", s);
      }

      [Fact]
      public void TableWriterWritesTwoHeaders()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] {
               new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell1" } } },
               new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell2" } } },
            } },
            ColumnDetails = new[] {
               new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String },
               new ColumnDetails { columnName = "harrumphflup", columnWidth = 10, isNullable = false, type = Data.DataType.String },
            }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.Contains("TestCell1", s);
         Assert.Contains("TestCell2", s);
      }

      [Fact]
      public void TableWriter_SimpleTable_WritesHeaderLines3LineBreaks()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String } }
         };

         target.Draw(table);
         mockOutputter.Verify(m => m.Write(It.Is<string>(s => s == Environment.NewLine)), Times.Exactly(3));
      }

      [Fact]
      public void TableWriterWritesHeaderLines()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.Equal(20, s.Count(c => c == '-'));
      }


      [Fact]
      public void TableWriterWritesHeaderRespectsColorChoice()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell", ForegroundColor = ConsoleColor.Green } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String } }
         };

         target.Draw(table);
         mockOutputter.Verify(m => m.SetForegroundColor(It.Is<ConsoleColor>(c => c == ConsoleColor.Green)), Times.Once());
      }


      [Fact]
      public void TableWriterWritesHeaderResetsColorChoice()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell", ForegroundColor = ConsoleColor.Green } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String } }
         };

         target.Draw(table);
         mockOutputter.Verify(m => m.ResetColor(), Times.Once());
      }
   }
}
