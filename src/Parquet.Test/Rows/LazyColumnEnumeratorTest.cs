using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Parquet.Data.Rows;
using Xunit;

namespace Parquet.Test.Rows
{
   public class LazyColumnEnumeratorTest
   {
      [Fact]
      public void Two_level_rep_levels()
      {
         //prepare columns with two items, each item has two inline items
         var dc = new DataColumn(new DataField<int>("openingHours") { MaxRepetitionLevel = 2 },
            new[]
            {
               1, 2, 3, 4,
               5, 6,

               7, 8, 9,
               10, 11, 12, 13

            },
            null,
            1,
            new[]
            {
               0, 2, 2, 2,
               1, 2,

               0, 2, 2,
               1, 2, 2, 2
            },
            2,
            null,
            null
            );

         var e = new LazyColumnEnumerator(dc);

         List<LazyColumnEnumerator> topLevel = e.ToEnumeratorList();
         Assert.Equal(2, topLevel.Count);

         List<LazyColumnEnumerator> row1 = topLevel[0].ToEnumeratorList();
         List<LazyColumnEnumerator> row2 = topLevel[1].ToEnumeratorList();
         Assert.Equal(2, row1.Count);
         Assert.Equal(2, row2.Count);

         Assert.Equal(new[] { 1, 2, 3, 4 }, row1[0].ToDataArray());
         Assert.Equal(new[] { 5, 6 }, row1[1].ToDataArray());

         Assert.Equal(new[] { 7, 8, 9 }, row2[0].ToDataArray());
         Assert.Equal(new[] { 10, 11, 12, 13 }, row2[1].ToDataArray());
      }

      [Fact]
      public void Simple_array()
      {
         var dc = new DataColumn(new DataField<int>("ids") { MaxRepetitionLevel = 1 },
         new[]
         {
            1, 2, 3, 4,
            5, 6
         },
         null,
         1,
         new[]
         {
            0, 1, 1, 1,
            0, 1
         },
         2,
         null,
         null);

         var e = new LazyColumnEnumerator(dc);

         List<LazyColumnEnumerator> topLevel = e.ToEnumeratorList();
         Assert.Equal(2, topLevel.Count);

         Assert.Equal(new[] { 1, 2, 3, 4 }, topLevel[0].ToDataArray());
         Assert.Equal(new[] { 5, 6 }, topLevel[1].ToDataArray());

      }

      [Fact]
      public void Empty_list()
      {
         var dc = new DataColumn(new DataField<int?>("ids") { MaxRepetitionLevel = 1 },
         new int?[]
         {
            1, 2,
            null,
            5, 6
         },
         null,
         1,
         new[]
         {
            0, 1,
            0,
            0, 1
         },
         2,
         null,
         null);

         var e = new LazyColumnEnumerator(dc);

         List<LazyColumnEnumerator> topLevel = e.ToEnumeratorList();
         Assert.Equal(3, topLevel.Count);

         Assert.Equal(2, topLevel[0].ToDataArray().Length);
         Assert.Empty(topLevel[1].ToDataArray());
         Assert.Equal(2, topLevel[2].ToDataArray().Length);
      }

      [Fact]
      public void Single_element_in_list_of_elements_in_a_structure()
      {
         var dc = new DataColumn(new DataField<double>("leftCategoriesOrThreshold") { MaxRepetitionLevel = 1 },
            new double[]
            {
               1.7,
               4.9,
               1.6
            },
            new int[]
            {
               0,
               0,
               0
            });

         var e = new LazyColumnEnumerator(dc);
         List<LazyColumnEnumerator> topLevel = e.ToEnumeratorList();
         Assert.Equal(3, topLevel.Count);

         double[] list0 = (double[])topLevel[0].ToDataArray();
         double[] list1 = (double[])topLevel[1].ToDataArray();
         double[] list2 = (double[])topLevel[2].ToDataArray();

         //three lists with one element
         Assert.Single(list0);
         Assert.Single(list1);
         Assert.Single(list2);

         //compare values
         Assert.Equal(1.7, list0[0], 3);
         Assert.Equal(4.9, list1[0], 3);
         Assert.Equal(1.6, list2[0], 3);
      }
   }
}
