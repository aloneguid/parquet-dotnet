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

         var packer = new RepetitionPack(schema);
         IList r = packer.Pack(flat, levels);

         Assert.Equal(2, r.Count);
         Assert.Equal(2, ((IList)r[0]).Count);
         Assert.Equal(2, ((IList)r[1]).Count);
         Assert.Equal(1, ((IList)r[0])[0]);
         Assert.Equal(2, ((IList)r[0])[1]);
         Assert.Equal(3, ((IList)r[1])[0]);
         Assert.Equal(4, ((IList)r[1])[1]);
      }
   }
}
