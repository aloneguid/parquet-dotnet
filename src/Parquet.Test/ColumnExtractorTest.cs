using System.Collections;
using System.Collections.Generic;
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
         var extractor = new ColumnExtractor();
         SimpleColumns[] classes = new[]
         {
            new SimpleColumns { Id = 1, Name = "First"}, new SimpleColumns { Id = 2, Name = "Second"}, new SimpleColumns { Id = 3, Name = "Third" }
         };

         List<DataColumn> columns = extractor.ExtractColumns(classes, schema);
         Assert.Equal(new[] { 1, 2, 3 }, columns[0].DefinedData);
         Assert.Equal(new[] { "First", "Second", "Third" }, columns[1].DefinedData);
      }

      [Fact]
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

         List<DataColumn> columns = extractor.ExtractColumns(ac, schema);

         Assert.Equal(new[] { 1, 2 }, columns[0].DefinedData);

         Assert.Equal(new[] { "Fiddler", "On", "The", "Roof" }, columns[1].DefinedData);
         Assert.Equal(new[] { 0, 1, 0, 1 }, columns[1].RepetitionLevels);
      }
   }
}
