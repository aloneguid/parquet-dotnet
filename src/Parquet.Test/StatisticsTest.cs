using System;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class StatisticsTest : TestBase
   {
      [Theory]
      [InlineData(typeof(int), new int[] { 4, 2, 1, 3, 1, 4 }, 6, 4)]
      [InlineData(typeof(string), new string[] { "one", "two", "one" }, 3, 2)]
      public void Distinct_stat_for_basic_data_types(Type dataType, Array values, long totalCount, long distinctCount)
      {
         var id = new DataField("id", dataType);

         DataColumn rc = WriteReadSingleColumn(id, new DataColumn(id, values));

         Assert.Equal(totalCount, rc.CalculateRowCount());
         Assert.Equal(distinctCount, rc.Statistics.DistinctCount);
         Assert.Equal(0, rc.Statistics.NullCount);
      }
   }
}
