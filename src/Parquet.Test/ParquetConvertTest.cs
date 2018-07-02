/*using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using NetBox.Generator;
using Parquet.Data;
using Parquet.File;
using Xunit;
using F = System.IO.File;

namespace Parquet.Test
{
   public class ParquetConvertTest : TestBase
   {
      [Fact]
      public void Serialise_single_row_group()
      {
         List<SimpleStructure> structures = Enumerable
            .Range(0, 10)
            .Select(i => new SimpleStructure { Id = i, Name = $"row {i}" })
            .ToList();

         using (var ms = new MemoryStream())
         {
            Schema schema = ParquetConvert.Serialize(structures, ms, compressionMethod: CompressionMethod.None);

            //F.WriteAllBytes("c:\\tmp\\ser.parquet", ms.ToArray());

            ms.Position = 0;

            using (var reader = new ParquetReader3(ms))
            {
               Assert.Equal(1, reader.RowGroupCount);

               using (ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0))
               {

                  DataColumn ids = rgr.ReadColumn(schema.DataFieldAt(0));
                  DataColumn names = rgr.ReadColumn(schema.DataFieldAt(1));

                  Assert.Equal(10, ids.Data.Length);
                  Assert.Equal(10, names.Data.Length);
               }
            }
         }
      }

      class SimpleStructure
      {
         public int Id { get; set; }

         public string Name { get; set; }
      }

   }
}*/