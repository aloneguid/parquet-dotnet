using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NetBox.Extensions;
using Parquet.Attributes;
using Parquet.Data;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test.Serialisation
{
   public class ParquetConvertTest : TestBase
   {
      [Fact]
      public async Task Serialise_Should_Exclude_IgnoredProperties_while_serialized_to_parquetfileAsync()
      {
         DateTime now = DateTime.Now;

         IEnumerable<StructureWithIgnoredProperties> structures = Enumerable
            .Range(0, 10)
            .Select(i => new StructureWithIgnoredProperties
            {
               Id = i,
               Name = $"row {i}",
               SSN = "000-00-0000",
               NonNullableDecimal = 100.534M,
               NullableDecimal = 99.99M,
               NonNullableDateTime = DateTime.Now,
               NullableDateTime = DateTime.Now,
               NullableInt = 111,
               NonNullableInt = 222
            }) ;

         using (var ms = new MemoryStream())
         {
            Schema schema = await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2).ConfigureAwait(false);

            ms.Position = 0;

            StructureWithIgnoredProperties[] structures2 = await ParquetConvert.DeserializeAsync<StructureWithIgnoredProperties>(ms).ConfigureAwait(false);

            StructureWithIgnoredProperties[] structuresArray = structures.ToArray();
            Func<Type, Object> GetDefaultValue = (type) => type.IsValueType ? Activator.CreateInstance(type) : null;

            for (int i = 0; i < 10; i++)
            {
               Assert.Equal(structuresArray[i].Id, structures2[i].Id);
               Assert.Equal(structuresArray[i].Name, structures2[i].Name);
               //As serialization ignored these below properties, deserilizing these should always be null(or type's default value).
               Assert.Equal(structures2[i].SSN, GetDefaultValue(typeof(string)));
               Assert.Equal(structures2[i].NonNullableInt, GetDefaultValue(typeof(int)));
               Assert.Equal(structures2[i].NullableInt, GetDefaultValue(typeof(int?)));
               Assert.Equal(structures2[i].NonNullableDecimal, GetDefaultValue(typeof(decimal)));
               Assert.Equal(structures2[i].NullableDecimal, GetDefaultValue(typeof(decimal?)));
               Assert.Equal(structures2[i].NonNullableDateTime, GetDefaultValue(typeof(DateTime)));
               Assert.Equal(structures2[i].NullableDateTime, GetDefaultValue(typeof(DateTime?)));
            }

         }
      }

      [Fact]
      public async Task Serialise_deserialise_all_typesAsync()
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
            Schema schema = await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2).ConfigureAwait(false);

            ms.Position = 0;

            SimpleStructure[] structures2 = await ParquetConvert.DeserializeAsync<SimpleStructure>(ms).ConfigureAwait(false);

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
      public async Task Serialise_deserialise_renamed_columnAsync()
      {
         IEnumerable<SimpleRenamed> structures = Enumerable
            .Range(0, 10)
            .Select(i => new SimpleRenamed
            {
               Id = i,
               PersonName = $"row {i}"
            });

         using (var ms = new MemoryStream())
         {
            Schema schema = await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2).ConfigureAwait(false);

            ms.Position = 0;

            SimpleRenamed[] structures2 = await ParquetConvert.DeserializeAsync<SimpleRenamed>(ms).ConfigureAwait(false);

            SimpleRenamed[] structuresArray = structures.ToArray();
            for (int i = 0; i < 10; i++)
            {
               Assert.Equal(structuresArray[i].Id, structures2[i].Id);
               Assert.Equal(structuresArray[i].PersonName, structures2[i].PersonName);
            }
         }
      }
      [Fact]
      public async Task Serialise_all_but_deserialise_only_few_propertiesAsync()
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
            Schema schema = await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2).ConfigureAwait(false);

            ms.Position = 0;

            SimpleStructure[] structuresArray = structures.ToArray();
            int rowGroupCount = 5; //based on our test input. 10 records with rowgroup size 2.
            for (int r = 0; r < rowGroupCount; r++)
            {
               SimpleStructureWithFewProperties[] rowGroupRecords = await ParquetConvert.DeserializeAsync<SimpleStructureWithFewProperties>(ms, rowGroupIndex: r).ConfigureAwait(false);
               Assert.Equal(2, rowGroupRecords.Length);

               Assert.Equal(structuresArray[2 * r].Id, rowGroupRecords[0].Id);
               Assert.Equal(structuresArray[2 * r].Name, rowGroupRecords[0].Name);
               Assert.Equal(structuresArray[2 * r + 1].Id, rowGroupRecords[1].Id);
               Assert.Equal(structuresArray[2 * r + 1].Name, rowGroupRecords[1].Name);

            }
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>("index", () => ParquetConvert.DeserializeAsync<SimpleStructure>(ms, 5)).ConfigureAwait(false);
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>("index", () => ParquetConvert.DeserializeAsync<SimpleStructure>(ms, 99999)).ConfigureAwait(false);
         }
      }
      [Fact]
      public async Task Serialise_read_and_deserialise_by_rowgroupAsync()
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
            Schema schema = await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2).ConfigureAwait(false);

            ms.Position = 0;

            SimpleStructure[] structuresArray = structures.ToArray();
            int rowGroupCount = 5; //based on our test input. 10 records with rowgroup size 2.
            for(int r = 0; r < rowGroupCount; r++)
            {
               SimpleStructure[] rowGroupRecords = await ParquetConvert.DeserializeAsync<SimpleStructure>(ms, rowGroupIndex: r).ConfigureAwait(false);
               Assert.Equal(2, rowGroupRecords.Length);

               Assert.Equal(structuresArray[2*r].Id, rowGroupRecords[0].Id);
               Assert.Equal(structuresArray[2*r].NullableId, rowGroupRecords[0].NullableId);
               Assert.Equal(structuresArray[2*r].Name, rowGroupRecords[0].Name);
               Assert.Equal(structuresArray[2*r].Date, rowGroupRecords[0].Date);
               Assert.Equal(structuresArray[2*r+1].Id, rowGroupRecords[1].Id);
               Assert.Equal(structuresArray[2*r+1].NullableId, rowGroupRecords[1].NullableId);
               Assert.Equal(structuresArray[2*r+1].Name, rowGroupRecords[1].Name);
               Assert.Equal(structuresArray[2*r+1].Date, rowGroupRecords[1].Date);

            }
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>("index",() => ParquetConvert.DeserializeAsync<SimpleStructure>(ms, 5)).ConfigureAwait(false);
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>("index",() => ParquetConvert.DeserializeAsync<SimpleStructure>(ms, 99999)).ConfigureAwait(false);
         }
      }

      [Fact]
      public async Task Serialize_deserialize_repeated_fieldAsync()
      {
         IEnumerable<SimpleRepeated> structures = Enumerable
            .Range(0, 10)
            .Select(i => new SimpleRepeated
            {
               Id = i,
               Areas = new int[] { i, 2, 3}
            });

         SimpleRepeated[] s = await ConvertSerialiseDeserialiseAsync(structures).ConfigureAwait(false);

         Assert.Equal(10, s.Length);

         Assert.Equal(0, s[0].Id);
         Assert.Equal(1, s[1].Id);

         Assert.Equal(new[] { 0, 2, 3 }, s[0].Areas);
         Assert.Equal(new[] { 1, 2, 3 }, s[1].Areas);
      }

      [Fact]
      public async Task Serialize_deserialize_empty_enumerableAsync()
      {
         IEnumerable<SimpleRepeated> structures = Enumerable.Empty<SimpleRepeated>();

         SimpleRepeated[] s = await ConvertSerialiseDeserialiseAsync(structures).ConfigureAwait(false);
   
         Assert.Empty(s);
      }

      [Fact]
      public async Task Serialize_structure_with_DateTimeAsync()
      {
         await TestRoundTripSerializationAsync(DateTime.UtcNow.RoundToSecond()).ConfigureAwait(false);
      }

      [Fact]
      public async Task Serialize_structure_with_nullable_DateTimeAsync()
      {
         await TestRoundTripSerializationAsync<DateTime?>(DateTime.UtcNow.RoundToSecond()).ConfigureAwait(false);
         await TestRoundTripSerializationAsync<DateTime?>(null).ConfigureAwait(false);
      }

      [Fact]
      public async Task Serialise_groupsAsync()
      {
         DateTime now = DateTime.Now;

         IEnumerable<SimpleStructure> structures = Enumerable
            .Range(start: 0, count: 10)
            .Select(i => new SimpleStructure
            {
               Id = i,
               NullableId = (i % 2 == 0) ? new int?() : new int?(i),
               Name = $"row {i}",
               Date = now.AddDays(i).RoundToSecond().ToUniversalTime()
            });

         using (var ms = new MemoryStream())
         {
            Schema schema = await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2).ConfigureAwait(false);

            ms.Position = 0;

            List<SimpleStructure[]> groups2 = new List<SimpleStructure[]>();
            await foreach (SimpleStructure[] item in ParquetConvert.DeserializeGroupsAsync<SimpleStructure>(ms))
            {
               groups2.Add(item);
            }
            
            Assert.Equal(10/2, groups2.Count); //groups = count/rowGroupSize

            SimpleStructure[] structuresArray = structures.ToArray();

            SimpleStructure[] structures2 = (
               from g in groups2
               from s in g
               select s
            ).ToArray();

            for (int i = 0; i < 10; i++)
            {
               Assert.Equal(structuresArray[i].Id, structures2[i].Id);
               Assert.Equal(structuresArray[i].NullableId, structures2[i].NullableId);
               Assert.Equal(structuresArray[i].Name, structures2[i].Name);
               Assert.Equal(structuresArray[i].Date, structures2[i].Date);
            }
         }
      }

      async Task TestRoundTripSerializationAsync<T>(T value)
      {
         StructureWithTestType<T> input = new StructureWithTestType<T>
         {
            Id = "1",
            TestValue = value,
         };

         Schema schema = SchemaReflector.Reflect<StructureWithTestType<T>>();

         using (MemoryStream stream = new MemoryStream())
         {
            await ParquetConvert.SerializeAsync(new StructureWithTestType<T>[] { input }, stream, schema).ConfigureAwait(false);

            stream.Position = 0;
            StructureWithTestType<T>[] output = await ParquetConvert.DeserializeAsync<StructureWithTestType<T>>(stream).ConfigureAwait(false);
            Assert.Single(output);
            Assert.Equal("1", output[0].Id);
            Assert.Equal(value, output[0].TestValue);
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
      public class SimpleStructureWithFewProperties
      {
         public int Id { get; set; }
         public string Name { get; set; }
      }
      public class StructureWithIgnoredProperties
      {
         public int Id { get; set; }
         public string Name { get; set; }

         [ParquetIgnore]
         public string SSN { get; set; }

         [ParquetIgnore]
         public DateTime NonNullableDateTime { get; set; }
         [ParquetIgnore]
         public DateTime? NullableDateTime { get; set; }

         [ParquetIgnore]
         public int NonNullableInt { get; set; }

         [ParquetIgnore]
         public int? NullableInt { get; set; }

         [ParquetIgnore]
         public decimal NonNullableDecimal { get; set; }
         [ParquetIgnore]
         public decimal? NullableDecimal { get; set; }
      }

      public class SimpleRenamed
      {
         public int Id { get; set; }

         [ParquetColumn("Name")]
         public string PersonName { get; set; }
      }

      public class StructureWithTestType<T>
      {
         T testValue;

         public string Id { get; set; }

         // public T TestValue { get; set; }
         public T TestValue { get { return testValue; } set { testValue = value; } }
      }
   }
}
