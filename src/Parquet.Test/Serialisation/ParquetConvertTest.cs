using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NetBox.Extensions;
using Parquet.Data;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test.Serialisation
{
   public class ParquetConvertTest : TestBase
   {
      [Fact]
      public void Serialise_deserialise_all_types()
      {
         DateTime now = DateTime.Now;

         IEnumerable<SimpleStructure> structures = Enumerable
            .Range(0, 10)
            .Select(i => new SimpleStructure
            {
               Id = i,
               NullableId = (i % 2 == 0) ? new int?() : new int?(i),
               Name = $"row {i}",
               Date = now.AddDays(i).RoundToSecond().ToUniversalTime()
            });

         using (var ms = new MemoryStream())
         {
            Schema schema = ParquetConvert.Serialize(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2);

            ms.Position = 0;

            SimpleStructure[] structures2 = ParquetConvert.Deserialize<SimpleStructure>(ms);

            SimpleStructure[] structuresArray = structures.ToArray();
            for (int i = 0; i < 10; i++)
            {
               Assert.Equal(structuresArray[i].Id, structures2[i].Id);
               Assert.Equal(structuresArray[i].NullableId, structures2[i].NullableId);
               Assert.Equal(structuresArray[i].Name, structures2[i].Name);
               Assert.Equal(structuresArray[i].Date, structures2[i].Date);
            }

         }
      }

      [Fact]
      public void Serialize_deserialize_repeated_field()
      {
         IEnumerable<SimpleRepeated> structures = Enumerable
            .Range(0, 10)
            .Select(i => new SimpleRepeated
            {
               Id = i,
               Areas = new int[] { i, 2, 3}
            });

         SimpleRepeated[] s = ConvertSerialiseDeserialise(structures);

         Assert.Equal(10, s.Length);

         Assert.Equal(0, s[0].Id);
         Assert.Equal(1, s[1].Id);

         Assert.Equal(new[] { 0, 2, 3 }, s[0].Areas);
         Assert.Equal(new[] { 1, 2, 3 }, s[1].Areas);
      }

      [Fact]
      public void Serialize_structure_with_DateTime()
      {
         StructureWithDateTime input = new StructureWithDateTime
         {
            Id = "1",
            Value = "empty string",
            CompletionDate = DateTime.UtcNow.RoundToSecond(),
         };

         Schema schema = SchemaReflector.Reflect<StructureWithDateTime>();

         using (MemoryStream stream = new MemoryStream())
         {
            ParquetConvert.Serialize<StructureWithDateTime>(new StructureWithDateTime[] { input }, stream, schema);

            stream.Position = 0;
            StructureWithDateTime[] output = ParquetConvert.Deserialize<StructureWithDateTime>(stream);
            Assert.Single(output);
            Assert.Equal("1", output[0].Id);
            Assert.Equal("empty string", output[0].Value);
            Assert.Equal(input.CompletionDate, output[0].CompletionDate);
         }
      }

      public class SimpleRepeated
      {
         public int Id { get; set; }

         public int[] Areas { get; set; }
      }

      public class SimpleStructure
      {
         public int Id { get; set; }

         public int? NullableId { get; set; }

         public string Name { get; set; }

         public DateTimeOffset Date { get; set; }
      }

      public class StructureWithDateTime
      {
         public string Id { get; set; }

         public string Value { get; set; }

         public DateTime CompletionDate { get; set; }
      }
   }
}
