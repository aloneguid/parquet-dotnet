using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Parquet.File.Values.Primitives;
using Xunit;

namespace Parquet.Test
{
   public class PrimitiveTypesTest
   {
      [Theory]
      [InlineData(100)]
      [InlineData(1000)]
      public void Write_loads_of_booleans_all_true(int count)
      {
         var ds = new DataSet(
            new DataField<int>("id"),
            new DataField<bool>("enabled"));

         //generate data
         for(int i = 0; i < count; i++)
         {
            ds.Add(i, true);
         }

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         for(int i = 0; i < count; i++)
         {
            Assert.True(ds1[i].GetBoolean(1), $"got FALSE at position {i}");
         }

      }

      [Fact]
      public void Interval_tests()
      {
         var ds = new DataSet(
            new DataField<int>("id"),
            new DataField<Interval>("interval"));
         ds.Add(1000, new Interval(1,2,3));

         DataSet ds1 = DataSetGenerator.WriteRead(ds);
         Interval retInterval = (Interval)ds1[0][1];
         Assert.Equal(1, retInterval.Months);
         Assert.Equal(2, retInterval.Days);
         Assert.Equal(3, retInterval.Millis);
      }
   }
}
