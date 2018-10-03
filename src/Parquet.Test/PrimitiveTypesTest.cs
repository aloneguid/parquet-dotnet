using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Parquet.File.Values.Primitives;
using Xunit;

namespace Parquet.Test
{
   public class PrimitiveTypesTest : TestBase
   {
      [Theory]
      [InlineData(100)]
      [InlineData(1000)]
      public void Write_loads_of_booleans_all_true(int count)
      {
         var id = new DataField<bool>("enabled");
         var schema = new Schema(id);

         bool[] data = new bool[count];
         //generate data
         for(int i = 0; i < count; i++)
         {
            data[i] = true;
         }

         DataColumn read = WriteReadSingleColumn(id, new DataColumn(id, data));

         for(int i = 0; i < count; i++)
         {
            Assert.True((bool)read.Data.GetValue(i), $"got FALSE at position {i}");
         }

      }
   }
}