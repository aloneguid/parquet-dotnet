using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class StatisticsTest : TestBase
   {
      class TestDesc
      {
         public Type Type { get; set; }

         public Array Data { get; set; }

         public long DistinctCount { get; set; }
      }

      private static Dictionary<string, TestDesc> NameToTest = new Dictionary<string, TestDesc>
      {
         ["int"] = new TestDesc
         {
            Type = typeof(int),
            Data = new int[] { 4, 2, 1, 3, 1, 4 },
            DistinctCount = 4
         },
         ["string"] = new TestDesc
         {
            Type = typeof(string),
            Data = new string[] { "one", "two", "one" },
            DistinctCount = 2
         },
         ["float"] = new TestDesc
         {
            Type = typeof(float),
            Data = new float[] { 1.23f, 2.1f, 0.5f, 0.5f },
            DistinctCount = 3
         },
         ["double"] = new TestDesc
         {
            Type = typeof(double),
            Data = new double[] { 1.23D, 2.1D, 0.5D, 0.5D },
            DistinctCount = 3
         },
         ["dateTime"] = new TestDesc
         {
            Type = typeof(DateTime),
            Data = new DateTimeOffset[]
            {
               new DateTimeOffset(new DateTime(2019, 12, 16)),
               new DateTimeOffset(new DateTime(2019, 12, 16)),
               new DateTimeOffset(new DateTime(2019, 12, 15)),
               new DateTimeOffset(new DateTime(2019, 12, 17))
            },
            DistinctCount = 3
         }
      };

      [Theory]
      [InlineData("int")]
      [InlineData("string")]
      [InlineData("float")]
      [InlineData("double")]
      [InlineData("dateTime")]
      public void Distinct_stat_for_basic_data_types(string name)
      {
         TestDesc test = NameToTest[name];

         var id = new DataField("id", test.Type);

         DataColumn rc = WriteReadSingleColumn(id, new DataColumn(id, test.Data));

         Assert.Equal(test.Data.Length, rc.CalculateRowCount());
         Assert.Equal(test.DistinctCount, rc.Statistics.DistinctCount);
         Assert.Equal(0, rc.Statistics.NullCount);
      }
   }
}
