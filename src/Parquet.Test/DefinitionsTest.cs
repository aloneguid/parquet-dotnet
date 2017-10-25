using System;
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
      public void Level4_definitions_packed()
      {
         var packer = new DefinitionPack(new SchemaElement<int>("cities") { MaxDefinitionLevel = 4 });

         List<int?> values = packer.Pack(new List<int> { 1, 2, 1, 2 }, new List<int> { 4, 4, 4, 4 }) as List<int?>;

         Assert.Equal(4, values.Count);
         Assert.Equal(Nullable(1, 2, 1, 2), values);
      }

      private List<T?> Nullable<T>(params T[] values) where T : struct
      {
         return values.Select(v => new T?(v)).ToList();
      }
   }
}
