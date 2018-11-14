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
   public class TableWriterColumnConstraintsTests
   {
      [Fact]
      public void TableWriterColumnConstraintsRespectedOnEachLine()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String } },
            Rows = new[] { new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "This is a test value." } } } } } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         string[] effectiveLines = s.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
         Assert.True(effectiveLines.All(l => l.Length == effectiveLines.First().Length), GetLengths(effectiveLines));
      }
      [Fact]
      public void TableWriterColumnConstraintsRespectedOnEachLineLargeWidth()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = 50, isNullable = false, type = Data.DataType.String } },
            Rows = new[] { new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "This is a test value." } } } } } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         string[] effectiveLines = s.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
         Assert.True(effectiveLines.All(l => l.Length == effectiveLines.First().Length), GetLengths(effectiveLines));
      }

      [Fact]
      public void TableWriterColumnConstraintsRespectedOverallWidth()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));
         int columnWidth = new Random().Next(95);

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = columnWidth, isNullable = false, type = Data.DataType.String } },
            Rows = new[] { new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "This is a test value." } } } } } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         string[] effectiveLines = s.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
         Assert.True(effectiveLines.All(l => l.Length == columnWidth + ("|".Length*2)), GetLengths(effectiveLines));
      }

      [Fact]
      public void TableWriterColumnConstraintsTrimsContent()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));
         int columnWidth = 10;

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = columnWidth, isNullable = false, type = Data.DataType.String } },
            Rows = new[] { new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "This is a test value." } } } } } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.Contains("This is...", s);
      }
      [Fact]
      public void TableWriterColumnConstraintsTrimsContentLonger()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));
         int columnWidth = 12;

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = columnWidth, isNullable = false, type = Data.DataType.String } },
            Rows = new[] { new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "This is a test value." } } } } } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.Contains("This is a...", s);
      }
      [Fact]
      public void TableWriterColumnConstraintsFullContentFits()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));
         int columnWidth = 21;

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = columnWidth, isNullable = false, type = Data.DataType.String } },
            Rows = new[] { new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "This is a test value." } } } } } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.Contains("This is a test value.", s);
      }
      [Fact]
      public void TableWriterColumnConstraintsPadsWithTooLargeColumn()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));
         int columnWidth = 25;

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = columnWidth, isNullable = false, type = Data.DataType.String } },
            Rows = new[] { new TableRow { Cells = new[] { new BasicTableCell {  ContentAreas = new[] { new BasicCellContent { Value = "This is a test value." } } } } } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.Contains("    This is a test value.", s);
      }

      private string GetLengths(string[] effectiveLines)
      {
         string output = "";
         foreach (string item in effectiveLines)
         {
            output += $"{item}[{item.Length}]{Environment.NewLine}";
         }
         return output;
      }
   }
}
