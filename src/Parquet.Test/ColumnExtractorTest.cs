using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Parquet.Data;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test
{
   public class ColumnExtractorTest
   {
      public class SimpleColumns
      {
         public int Id { get; set; }

         public string Name { get; set; }

         public int? AltId { get; set; }
      }

      public class ArrayColumns
      {
         public int Id { get; set; }

         public string[] Addresses { get; set; }
      }

      [Fact]
      public void Extract_simple_columns()
      {
         Schema schema = new SchemaReflector(typeof(SimpleColumns)).Reflect();
         var extractor = new DataColumnBuilder();
         SimpleColumns[] classes = new[]
         {
            new SimpleColumns { Id = 1, Name = "First", AltId = 1},
            new SimpleColumns { Id = 2, Name = "Second", AltId = null},
            new SimpleColumns { Id = 3, Name = "Third", AltId = 3 }
         };

         List<DataColumn> columns = extractor.BuildColumns(classes, schema).ToList();
         Assert.Equal(new[] { 1, 2, 3 }, columns[0].Data);
         Assert.Equal(new[] { "First", "Second", "Third" }, columns[1].Data);
         Assert.Equal(new int?[] { 1, null, 3 }, columns[2].Data);
      }

      /*[Fact]
      public void Extract_array_columns()
      {
         Schema schema = SchemaReflector.Reflect<ArrayColumns>();
         Assert.Equal(2, schema.Length);
         var extractor = new ColumnExtractor();
         ArrayColumns[] ac = new[]
         {
            new ArrayColumns
            {
               Id = 1,
               Addresses = new[] { "Fiddler", "On" }
            },
            new ArrayColumns
            {
               Id = 2,
               Addresses = new[] { "The", "Roof" }
            }
         };

         List<DataColumn> columns = extractor.ExtractColumns(ac, schema).ToList();

         Assert.Equal(new[] { 1, 2 }, columns[0].Data);

         Assert.Equal(new[] { "Fiddler", "On", "The", "Roof" }, columns[1].Data);
         Assert.Equal(new[] { 0, 1, 0, 1 }, columns[1].RepetitionLevels);
      }*/
   }
}