using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
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
   }
}
