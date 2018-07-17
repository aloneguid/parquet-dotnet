using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using NetBox.Generator;
using Parquet.Data;
using Parquet.File;
using Xunit;
using F = System.IO.File;

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

            using (var reader = new ParquetReader(ms))
            {
               Assert.Equal(1, reader.RowGroupCount);

               using (ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0))
               {

                  DataColumn idsColumn = rgr.ReadColumn(schema.DataFieldAt(0));
                  DataColumn namesColumns = rgr.ReadColumn(schema.DataFieldAt(1));

                  Assert.Equal(10, idsColumn.Data.Length);
                  Assert.Equal(10, namesColumns.Data.Length);

                  int[] ids = (int[])idsColumn.Data;
                  string[] names = (string[])namesColumns.Data;

                  for(int i = 0; i < 10; i++)
                  {
                     Assert.Equal(i, ids[i]);
                     Assert.Equal($"row {i}", names[i]);
                  }
               }
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