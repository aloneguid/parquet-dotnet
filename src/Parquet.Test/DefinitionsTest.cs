using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.Data;
using Parquet.File;
using Xunit;

namespace Parquet.Test
{
   public class DefinitionsTest
   {
      [Fact]
      public void Level4_definitions_packed_when_none_are_null()
      {
         var packer = new DefinitionPack(new SchemaElement<int?>("cities") { MaxDefinitionLevel = 4 });

         var values = new List<int?> { 1, 2, 1, 2 };
         packer.Pack(values, new List<int> { 4, 4, 4, 4 });

         Assert.Equal(4, values.Count);
         Assert.Equal(Nullable<int>(1, 2, 1, 2), values);
      }

      [Fact]
      public void First_and_second_is_null_packed()
      {
         var packer = new DefinitionPack(new SchemaElement<int?>("a") { MaxDefinitionLevel = 1 });

         var values = new List<int?> { 1, 2 };
         packer.Pack(values, new List<int> { 0, 0, 1, 1 });

         Assert.Equal(4, values.Count);
         Assert.Equal(Nullable<int>(null, null, 1, 2), values);
      }

      [Fact]
      public void First_and_second_is_null_unpacked()
      {
         var packer = new DefinitionPack(new SchemaElement<int?>("a") { MaxDefinitionLevel = 1 });
         IList unpacked = packer.Unpack(new List<int?> { null, null, 1, 2 }, out List<int> definitions);

         Assert.Equal(new int[] { 1, 2 }, unpacked);
         Assert.Equal(new int[] { 0, 0, 1, 1 }, definitions);
      }

      [Fact]
      public void First_and_lastis_null_packed()
      {
         var packer = new DefinitionPack(new SchemaElement<int?>("a") { MaxDefinitionLevel = 1 });

         var values = new List<int?> { 1, 2 };
         packer.Pack(values, new List<int> { 0, 1, 1, 0 });

         Assert.Equal(4, values.Count);
         Assert.Equal(Nullable<int>(null, 1, 2, null), values);
      }


      private List<T?> Nullable<T>(params object[] values) where T : struct
      {
         return values.Select(v => v == null ? new T?() : new T?((T)v)).ToList();
      }
   }
}
