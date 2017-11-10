using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Parquet.File;
using Xunit;

namespace Parquet.Test
{
   public class RepetitionsTest
   {
      [Fact]
      public void Level1_repetitions_packed()
      {
         var levels = new List<int> { 0, 1, 0, 1 };
         var flat = new List<int> { 1, 2, 3, 4 };
         var schema = new SchemaElement<int>("line1") { MaxRepetitionLevel = 1 };

         var packer = new RepetitionPack();
         IList r = packer.Pack(schema, flat, levels);

         Assert.Equal(2, r.Count);
         Assert.Equal(2, ((IList)r[0]).Count);
         Assert.Equal(2, ((IList)r[1]).Count);
         Assert.Equal(1, ((IList)r[0])[0]);
         Assert.Equal(2, ((IList)r[0])[1]);
         Assert.Equal(3, ((IList)r[1])[0]);
         Assert.Equal(4, ((IList)r[1])[1]);
      }

      //[Fact]
      public void Level1_repetitions_list_packed()
      {
         var levels = new List<int> { 0, 1, 0, 1 };
         var flat = new List<int> { 1, 2, 3, 4 };

         var rlist = new ListWithRepetitions<int>(flat, levels, 1);

         Assert.Equal(2, rlist.Count);
      }

      [Fact]
      public void Level1_repetitions_unpacked()
      {
         var schema = new SchemaElement<int>("line1") { MaxRepetitionLevel = 1 };
         var packer = new RepetitionPack();

         IList flatValues = packer.Unpack(schema,
            new List<List<int>>
            {
               new List<int>{ 1, 2 },
               new List<int>{ 3, 4 }
            },
            out List<int> levels
            );

         Assert.Equal(4, flatValues.Count);
         Assert.Equal(4, levels.Count);

         Assert.Equal(new[] { 0, 1, 0, 1 }, levels);
         Assert.Equal(new[] { 1, 2, 3, 4 }, flatValues);
      }

      [Fact]
      public void Level2_repetitions_packed()
      {
         var levels = new List<int>
         {
            0, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            1, 2, 2, 2, 2, 2, 2,
            0, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            1, 2, 2, 2, 2, 2, 2
         };
         var flat = new List<int>
         {
            9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            6, 7, 19, 20, 21, 22, 23,
            9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            6, 7, 19, 20, 21, 22, 23
         };
         var schema = new SchemaElement<int>("hours") { MaxRepetitionLevel = 2 };

         var packer = new RepetitionPack();
         IList r = packer.Pack(schema, flat, levels);

         Assert.Equal(2, r.Count);

         //first struct
         IList s1 = GetList(r, 0);
         Assert.Equal(2, s1.Count);
         IList s11 = GetList(r, 0, 0);
         Assert.Equal(10, s11.Count);
         Assert.Equal(new int[] { 9, 10, 11, 12, 13, 14, 15, 16, 17, 18 }, s11);
         IList s12 = GetList(r, 0, 1);
         Assert.Equal(7, s12.Count);
         Assert.Equal(new int[] { 6, 7, 19, 20, 21, 22, 23 }, s12);

         //second struct
         IList s2 = GetList(r, 1);
         Assert.Equal(2, s2.Count);
         IList s21 = GetList(r, 1, 0);
         Assert.Equal(10, s21.Count);
         Assert.Equal(new int[] { 9, 10, 11, 12, 13, 14, 15, 16, 17, 18 }, s21);
         IList s22 = GetList(r, 1, 1);
         Assert.Equal(7, s22.Count);
         Assert.Equal(new int[] { 6, 7, 19, 20, 21, 22, 23 }, s22);
      }

      [Fact]
      public void Level2_repetitions_unpacked()
      {
         var schema = new SchemaElement<int>("hours") { MaxRepetitionLevel = 2 };
         var packer = new RepetitionPack();

         IList flatValues = packer.Unpack(schema,
            new List<List<List<int>>>
            {
               new List<List<int>>
               {
                  new List<int>{ 9, 10, 11, 12, 13, 14, 15, 16, 17, 18 },
                  new List<int>{ 6, 7, 19, 20, 21, 22, 23 }
               },
               new List<List<int>>
               {
                  new List<int>{ 9, 10, 11, 12, 13, 14, 15, 16, 17, 18 },
                  new List<int>{ 6, 7, 19, 20, 21, 22, 23 }
               },
            },
            out List<int> levels
            );

         Assert.Equal(34, flatValues.Count);
         Assert.Equal(34, levels.Count);

         Assert.Equal(new List<int>
         {
            0, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            1, 2, 2, 2, 2, 2, 2,
            0, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            1, 2, 2, 2, 2, 2, 2
         }, levels);

         Assert.Equal(new List<int>
         {
            9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            6, 7, 19, 20, 21, 22, 23,
            9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            6, 7, 19, 20, 21, 22, 23
         }, flatValues);
      }

      private static IList GetList(IList root, params int[] levels)
      {
         foreach(int l in levels)
         {
            root = (IList)root[l];
         }

         return root;
      }
   }
}
