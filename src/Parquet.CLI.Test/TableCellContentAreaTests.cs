using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.CLI.Models.Tabular;
using Xunit;

namespace Parquet.CLI.Test
{
   public class TableCellContentAreaTests
   {
      [Fact]
      public void TableCellCorrectlySplitsAtBreakingRule()
      {
         var target = new BasicTableCell
         {
            ContentAreas = new ICellContent[] {
                  new BasicCellContent { Value = "TestCell" },
                  new BreakingRuleContentArea(),
                  new BasicCellContent { Value = "Line2" }
               }
         };

         Dictionary<int, List<ICellContent>> actual = target.GetContentAreasInLines();
         Assert.Equal(2, actual.Count);
      }

      [Fact]
      public void TableCellMultipleBreakingRules()
      {
         var target = new BasicTableCell
         {
            ContentAreas = new ICellContent[] {
                  new BreakingRuleContentArea(),
                  new BreakingRuleContentArea(),
                  new BreakingRuleContentArea()
               }
         };

         Dictionary<int, List<ICellContent>> actual = target.GetContentAreasInLines();
         Assert.Equal(4, actual.Count);
      }
      [Fact]
      public void TableCellMultipleBreakingRulesButNoContentWillRender()
      {
         var target = new BasicTableCell
         {
            ContentAreas = new ICellContent[] {
                  new BreakingRuleContentArea(),
                  new BreakingRuleContentArea(),
                  new BreakingRuleContentArea()
               }
         };

         Dictionary<int, List<ICellContent>> actual = target.GetContentAreasInLines();
         Assert.True(actual.All(t=> t.Value.Count == 0));
      }
   }
}
