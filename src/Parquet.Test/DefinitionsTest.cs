using System;
using System.Collections.Generic;
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

      }
   }
}
