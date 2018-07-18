using System.IO;
using System.Linq;
using Parquet.Data;
using Xunit;

namespace Parquet.Test.Serialisation
{
   public class ParquetConvertTest : TestBase
   {
      [Fact]
      public void Serialise_single_row_group()
      {
         SimpleStructure[] structures = Enumerable
            .Range(0, 10)
            .Select(i => new SimpleStructure { Id = i, Name = $"row {i}" })
            .ToArray();

         using (var ms = new MemoryStream())
         {
            Schema schema = ParquetConvert.Serialize(structures, ms, compressionMethod: CompressionMethod.None);

            ms.Position = 0;

            SimpleStructure[] structures2 = ParquetConvert.Deserialize<SimpleStructure>(ms);

            for(int i = 0; i < 10; i++)
            {
               Assert.Equal(structures[i].Id, structures2[i].Id);
               //Assert.Equal(structures[i].Name, structures2[i].Name);
            }

         }
      }

      public class SimpleStructure
      {
         public int Id { get; set; }

         public string Name { get; set; }
      }

   }
}