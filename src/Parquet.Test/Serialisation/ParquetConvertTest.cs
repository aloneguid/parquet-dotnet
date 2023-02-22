using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Parquet.Schema;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test.Serialisation {
    public class ParquetConvertTest : TestBase {

        [Fact]
        public async Task SmokeTest() {
            SimpleStructure[] structures = Enumerable
                .Range(0, 1000)
                .Select(i => new SimpleStructure {
                    Id = i,
                    Name = $"row {i}"
                })
                .ToArray();

            await ParquetConvert.SerializeAsync(structures, new MemoryStream());
        }

        [Fact]
        public async Task Serialise_Should_Exclude_IgnoredProperties_while_serialized_to_parquetfile() {
            DateTime now = DateTime.Now;

            IEnumerable<StructureWithIgnoredProperties> structures = Enumerable
               .Range(0, 10)
               .Select(i => new StructureWithIgnoredProperties {
                   Id = i,
                   Name = $"row {i}",
                   SSN = "000-00-0000",
                   NonNullableDecimal = 100.534M,
                   NullableDecimal = 99.99M,
                   NonNullableDateTime = DateTime.Now,
                   NullableDateTime = DateTime.Now,
                   NullableInt = 111,
                   NonNullableInt = 222
               });

            using(var ms = new MemoryStream()) {
                ParquetSchema schema = await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2);

                ms.Position = 0;

                StructureWithIgnoredProperties[] structures2 = await ParquetConvert.DeserializeAsync<StructureWithIgnoredProperties>(ms);

                StructureWithIgnoredProperties[] structuresArray = structures.ToArray();
                Func<Type, object?> GetDefaultValue = (type) => type.IsValueType ? Activator.CreateInstance(type) : null;

                for(int i = 0; i < 10; i++) {
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
        public async Task Serialise_deserialise_all_types() {
            DateTime now = DateTime.Now;

            IEnumerable<SimpleStructure> structures = Enumerable
               .Range(0, 10)
               .Select(i => new SimpleStructure {
                   Id = i,
                   NullableId = (i % 2 == 0) ? new int?() : new int?(i),
                   Name = $"row {i}",
                   Date = now.AddDays(i).RoundToSecond().ToUniversalTime()
               });

            using(var ms = new MemoryStream()) {
                ParquetSchema schema = await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2);

                ms.Position = 0;

                SimpleStructure[] structures2 = await ParquetConvert.DeserializeAsync<SimpleStructure>(ms);

                SimpleStructure[] structuresArray = structures.ToArray();
                for(int i = 0; i < 10; i++) {
                    Assert.Equal(structuresArray[i].Id, structures2[i].Id);
                    Assert.Equal(structuresArray[i].NullableId, structures2[i].NullableId);
                    Assert.Equal(structuresArray[i].Name, structures2[i].Name);
                    Assert.Equal(structuresArray[i].Date, structures2[i].Date);
                }
            }
        }

        [Fact]
        public async Task Serialize_append_deserialise() {
            DateTime now = DateTime.Now;

            IEnumerable<SimpleStructure> structures = Enumerable
               .Range(0, 5)
               .Select(i => new SimpleStructure {
                   Id = i,
                   NullableId = (i % 2 == 0) ? new int?() : new int?(i),
                   Name = $"row {i}",
                   Date = now.AddDays(i).RoundToSecond().ToUniversalTime()
               });

            IEnumerable<SimpleStructure> appendStructures = Enumerable
               .Range(5, 5)
               .Select(i => new SimpleStructure {
                   Id = i,
                   NullableId = (i % 2 == 0) ? new int?() : new int?(i),
                   Name = $"row {i}",
                   Date = now.AddDays(i).RoundToSecond().ToUniversalTime()
               });

            using(var ms = new MemoryStream()) {
                await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2);

                await ParquetConvert.SerializeAsync(appendStructures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2, append: true);

                ms.Position = 0;

                SimpleStructure[] structures2 = await ParquetConvert.DeserializeAsync<SimpleStructure>(ms);

                SimpleStructure[] structuresArray = structures.Concat(appendStructures).ToArray();

                Assert.Equal(structuresArray.Length, structures2.Length);
                for(int i = 0; i < structuresArray.Length; i++) {
                    Assert.Equal(structuresArray[i].Id, structures2[i].Id);
                    Assert.Equal(structuresArray[i].NullableId, structures2[i].NullableId);
                    Assert.Equal(structuresArray[i].Name, structures2[i].Name);
                    Assert.Equal(structuresArray[i].Date, structures2[i].Date);
                }
            }
        }

        /// <summary>
        /// Write to a new file, then append to it. Expect to deserialize it successfully.
        /// </summary>
        [Fact]
        public async Task Serialize_new_file_append_deserialize() {
            DateTime now = DateTime.Now;

            // Unique filename to avoid issues with existing files from previous test runs
            string filename = $"testfile-{Guid.NewGuid()}.parquet";

            SimpleStructure[] structures = Enumerable
               .Range(0, 5)
               .Select(i => new SimpleStructure {
                   Id = i,
                   NullableId = (i % 2 == 0) ? new int?() : new int?(i),
                   Name = $"row {i}",
                   Date = now.AddDays(i).RoundToSecond().ToUniversalTime()
               })
               .ToArray();

            SimpleStructure[] appendStructures = Enumerable
               .Range(5, 5)
               .Select(i => new SimpleStructure {
                   Id = i,
                   NullableId = (i % 2 == 0) ? new int?() : new int?(i),
                   Name = $"row {i}",
                   Date = now.AddDays(i).RoundToSecond().ToUniversalTime()
               })
               .ToArray();

            await ParquetConvert.SerializeAsync(structures, filename, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2);

            await ParquetConvert.SerializeAsync(appendStructures, filename, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2, append: true);

            SimpleStructure[] structuresRead = await ParquetConvert.DeserializeAsync<SimpleStructure>(filename);

            SimpleStructure[] structuresArray = structures.Concat(appendStructures).ToArray();

            Assert.Equal(structuresArray.Length, structuresRead.Length);
            for(int i = 0; i < structuresArray.Length; i++) {
                Assert.Equal(structuresArray[i].Id, structuresRead[i].Id);
                Assert.Equal(structuresArray[i].NullableId, structuresRead[i].NullableId);
                Assert.Equal(structuresArray[i].Name, structuresRead[i].Name);
                Assert.Equal(structuresArray[i].Date, structuresRead[i].Date);
            }

            System.IO.File.Delete(filename);
        }

        /// <summary>
        /// Write to a new file, then overwrite it. Expect to deserialize it successfully.
        /// </summary>
        [Fact]
        public async Task Serialize_overwrite_existing_file_deserialize() {
            DateTime now = DateTime.Now;

            // Unique filename to avoid issues with existing files from previous test runs
            string filename = $"testfile-{Guid.NewGuid()}.parquet";

            SimpleStructure[] structures = Enumerable
               .Range(0, 5)
               .Select(i => new SimpleStructure {
                   Id = i,
                   NullableId = (i % 2 == 0) ? new int?() : new int?(i),
                   Name = $"row {i}",
                   Date = now.AddDays(i).RoundToSecond().ToUniversalTime()
               })
               .ToArray();

            SimpleStructure[] overwriteStructures = Enumerable
               .Range(5, 5)
               .Select(i => new SimpleStructure {
                   Id = i,
                   NullableId = (i % 2 == 0) ? new int?() : new int?(i),
                   Name = $"row {i}",
                   Date = now.AddDays(i).RoundToSecond().ToUniversalTime()
               })
               .ToArray();

            await ParquetConvert.SerializeAsync(structures, filename, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2);

            await ParquetConvert.SerializeAsync(overwriteStructures, filename, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2, append: false);

            SimpleStructure[] structuresRead = await ParquetConvert.DeserializeAsync<SimpleStructure>(filename);

            Assert.Equal(overwriteStructures.Length, structuresRead.Length);
            for(int i = 0; i < overwriteStructures.Length; i++) {
                Assert.Equal(overwriteStructures[i].Id, structuresRead[i].Id);
                Assert.Equal(overwriteStructures[i].NullableId, structuresRead[i].NullableId);
                Assert.Equal(overwriteStructures[i].Name, structuresRead[i].Name);
                Assert.Equal(overwriteStructures[i].Date, structuresRead[i].Date);
            }

            System.IO.File.Delete(filename);
        }

        /// <summary>
        /// Write to a new file, then append to it with a changed schema. Expect thrown <see cref="ParquetException"/>.
        /// </summary>
        [Fact]
        public async Task Serialize_existing_file_append_changedschema_throw() {
            DateTime now = DateTime.Now;

            // Unique filename to avoid issues with existing files from previous test runs
            string filename = $"testfile-{Guid.NewGuid()}.parquet";

            SimpleStructure[] structures = Enumerable
               .Range(0, 5)
               .Select(i => new SimpleStructure {
                   Id = i,
                   NullableId = (i % 2 == 0) ? new int?() : new int?(i),
                   Name = $"row {i}",
                   Date = now.AddDays(i).RoundToSecond().ToUniversalTime()
               })
               .ToArray();

            SimpleChangedStructure[] appendStructures = Enumerable
               .Range(5, 5)
               .Select(i => new SimpleChangedStructure {
                   Id = i,
                   NullableId = (i % 2 == 0) ? new int?() : new int?(i),
                   Name = $"row {i}",
                   Offset = TimeSpan.FromMinutes(i),
               })
               .ToArray();

            await ParquetConvert.SerializeAsync(structures, filename, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2);

            Exception exception = await Assert.ThrowsAsync<ParquetException>(async () => await ParquetConvert.SerializeAsync(appendStructures, filename, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2, append: true));

            Assert.Contains("passed schema does not match existing file schema", exception.Message);

            System.IO.File.Delete(filename);
        }

        [Fact]
        public async Task Serialise_deserialise_renamed_column() {
            IEnumerable<SimpleRenamed> structures = Enumerable
               .Range(0, 10)
               .Select(i => new SimpleRenamed {
                   Id = i,
                   PersonName = $"row {i}",
                   NullableDecimal = (i % 3 == 0) ? null : (decimal)i / 3
               });

            using(var ms = new MemoryStream()) {
                ParquetSchema schema = await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2);

                ms.Position = 0;

                SimpleRenamed[] structures2 = await ParquetConvert.DeserializeAsync<SimpleRenamed>(ms);

                SimpleRenamed[] structuresArray = structures.ToArray();
                for(int i = 0; i < 10; i++) {
                    Assert.Equal(structuresArray[i].Id, structures2[i].Id);
                    Assert.Equal(structuresArray[i].PersonName, structures2[i].PersonName);
                    Assert.Equal(structuresArray[i].NullableDecimal.HasValue, structures2[i].NullableDecimal.HasValue);
                    if(structuresArray[i].NullableDecimal.HasValue)
                        Assert.Equal(structuresArray[i].NullableDecimal!.Value, structures2[i].NullableDecimal!.Value, 5);
                }
            }
        }

        [Fact]
        public async Task Serialise_deserialise_listfield_column() {
            IEnumerable<SimpleWithListField> structures = Enumerable
               .Range(0, 10)
               .Select(i => new SimpleWithListField {
                   col1 = new int[] { i - 1, i, i + 1 },
                   col2 = new int[] { 10 + i - 1, 10 + i, 10 + i + 1 },
                   col3 = new int[] { 100 + i - 1, 100 + i, 100 + i + 1 }
               });

            using(var ms = new MemoryStream()) {
                ParquetSchema schema = await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2000);

                Assert.Collection(schema.Fields,
                   (col) => {
                       if(!(col is ListField f)) {
                           Assert.True(false, $"{col.Name} is not a ListField");
                       }
                       else {
                           Assert.Equal("col1", f.Name);
                           Assert.Equal("bag", f.ContainerName);
                           Assert.Equal("array_element", f.Item.Name);
                           Assert.Equal("col1", f.Item.ClrPropName);
                       }
                   },
                   (col) => {
                       if(!(col is ListField f)) {
                           Assert.True(false, $"{col.Name} is not a ListField");
                       }
                       else {
                           Assert.Equal("col2", f.Name);
                           Assert.Equal("list", f.ContainerName);
                           Assert.Equal("col2", f.Item.Name);
                           Assert.Equal("col2", f.Item.ClrPropName);
                       }
                   },
                   (col) => {
                       Assert.True(col is DataField, $"{col.Name} is not a ListField");
                   }
                );

                ms.Position = 0;

                SimpleWithListField[] structures2 = await ParquetConvert.DeserializeAsync<SimpleWithListField>(ms);

                SimpleWithListField[] structuresArray = structures.ToArray();
                for(int i = 0; i < 10; i++) {
                    Assert.True(Enumerable.SequenceEqual(structuresArray[i].col1!, structures2[i].col1!));
                    Assert.True(Enumerable.SequenceEqual(structuresArray[i].col2!, structures2[i].col2!));
                    Assert.True(Enumerable.SequenceEqual(structuresArray[i].col3!, structures2[i].col3!));
                }
            }
        }
        [Fact]
        public async Task Serialise_all_but_deserialise_only_few_properties() {
            DateTime now = DateTime.Now;

            IEnumerable<SimpleStructure> structures = Enumerable
               .Range(0, 10)
               .Select(i => new SimpleStructure {
                   Id = i,
                   NullableId = (i % 2 == 0) ? new int?() : new int?(i),
                   Name = $"row {i}",
                   Date = now.AddDays(i).RoundToSecond().ToUniversalTime()
               });

            using(var ms = new MemoryStream()) {
                ParquetSchema schema = await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2);

                ms.Position = 0;

                SimpleStructure[] structuresArray = structures.ToArray();
                int rowGroupCount = 5; //based on our test input. 10 records with rowgroup size 2.
                for(int r = 0; r < rowGroupCount; r++) {
                    SimpleStructureWithFewProperties[] rowGroupRecords = await ParquetConvert.DeserializeAsync<SimpleStructureWithFewProperties>(ms, rowGroupIndex: r);
                    Assert.Equal(2, rowGroupRecords.Length);

                    Assert.Equal(structuresArray[2 * r].Id, rowGroupRecords[0].Id);
                    Assert.Equal(structuresArray[2 * r].Name, rowGroupRecords[0].Name);
                    Assert.Equal(structuresArray[(2 * r) + 1].Id, rowGroupRecords[1].Id);
                    Assert.Equal(structuresArray[(2 * r) + 1].Name, rowGroupRecords[1].Name);

                }
                await Assert.ThrowsAsync<ArgumentOutOfRangeException>("index", () => ParquetConvert.DeserializeAsync<SimpleStructure>(ms, 5));
                await Assert.ThrowsAsync<ArgumentOutOfRangeException>("index", () => ParquetConvert.DeserializeAsync<SimpleStructure>(ms, 99999));
            }
        }

        [Fact]
        public async Task Serialise_read_and_deserialise_by_rowgroup() {
            DateTime now = DateTime.Now;

            IEnumerable<SimpleStructure> structures = Enumerable
               .Range(0, 10)
               .Select(i => new SimpleStructure {
                   Id = i,
                   NullableId = (i % 2 == 0) ? new int?() : new int?(i),
                   Name = $"row {i}",
                   Date = now.AddDays(i).RoundToSecond().ToUniversalTime()
               });

            using(var ms = new MemoryStream()) {
                ParquetSchema schema = await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2);

                ms.Position = 0;

                SimpleStructure[] structuresArray = structures.ToArray();
                int rowGroupCount = 5; //based on our test input. 10 records with rowgroup size 2.
                for(int r = 0; r < rowGroupCount; r++) {
                    SimpleStructure[] rowGroupRecords = await ParquetConvert.DeserializeAsync<SimpleStructure>(ms, rowGroupIndex: r);
                    Assert.Equal(2, rowGroupRecords.Length);

                    Assert.Equal(structuresArray[2 * r].Id, rowGroupRecords[0].Id);
                    Assert.Equal(structuresArray[2 * r].NullableId, rowGroupRecords[0].NullableId);
                    Assert.Equal(structuresArray[2 * r].Name, rowGroupRecords[0].Name);
                    Assert.Equal(structuresArray[2 * r].Date, rowGroupRecords[0].Date);
                    Assert.Equal(structuresArray[(2 * r) + 1].Id, rowGroupRecords[1].Id);
                    Assert.Equal(structuresArray[(2 * r) + 1].NullableId, rowGroupRecords[1].NullableId);
                    Assert.Equal(structuresArray[(2 * r) + 1].Name, rowGroupRecords[1].Name);
                    Assert.Equal(structuresArray[(2 * r) + 1].Date, rowGroupRecords[1].Date);

                }
                await Assert.ThrowsAsync<ArgumentOutOfRangeException>("index", () => ParquetConvert.DeserializeAsync<SimpleStructure>(ms, 5));
                await Assert.ThrowsAsync<ArgumentOutOfRangeException>("index", () => ParquetConvert.DeserializeAsync<SimpleStructure>(ms, 99999));
            }
        }

        [Fact]
        public async Task Serialize_deserialize_repeated_field() {
            IEnumerable<SimpleRepeated> structures = Enumerable
               .Range(0, 10)
               .Select(i => new SimpleRepeated {
                   Id = i,
                   Areas = new int[] { i, 2, 3 }
               });

            SimpleRepeated[] s = await ConvertSerialiseDeserialise(structures);

            Assert.Equal(10, s.Length);

            Assert.Equal(0, s[0].Id);
            Assert.Equal(1, s[1].Id);

            Assert.Equal(new[] { 0, 2, 3 }, s[0].Areas);
            Assert.Equal(new[] { 1, 2, 3 }, s[1].Areas);
        }

        [Fact]
        public async Task Serialize_deserialize_empty_enumerable() {
            IEnumerable<SimpleRepeated> structures = Enumerable.Empty<SimpleRepeated>();

            SimpleRepeated[] s = await ConvertSerialiseDeserialise(structures);

            Assert.Empty(s);
        }

        [Fact]
        public async Task Serialize_explicit_schema_dataFields_with_non_null_propertyNames() {
            // unit test for https://github.com/aloneguid/parquet-dotnet/issues/179

            // create items to be serialised
            List<SimpleWithDateTimeAndDecimal> items = new List<SimpleWithDateTimeAndDecimal>() {
                new SimpleWithDateTimeAndDecimal() {
                    DateTimeValue = DateTime.UtcNow,
                    DecimalValue = 123m,
                    Int32Value = 456,
                },
                new SimpleWithDateTimeAndDecimal() {
                    DateTimeValue = DateTime.Now,
                    DecimalValue = 234m,
                    Int32Value = 567,
                },
            };

            // create schema - set DataField.propertyName to non-null strings
            ParquetSchema schema = new ParquetSchema(
                new DateTimeDataField(
                    nameof(SimpleWithDateTimeAndDecimal.DateTimeValue),
                    DateTimeFormat.Date,
                    null,
                    null,
                    nameof(SimpleWithDateTimeAndDecimal.DateTimeValue)),
                new DecimalDataField(
                    nameof(SimpleWithDateTimeAndDecimal.DecimalValue),
                    38, 18,
                    false,
                    null, null,
                    nameof(SimpleWithDateTimeAndDecimal.DecimalValue)),
                new DataField(
                    nameof(SimpleWithDateTimeAndDecimal.Int32Value),
                    typeof(int),
                    null, null,
                    nameof(SimpleWithDateTimeAndDecimal.Int32Value)));

            // serialise items
            using(MemoryStream ms = new MemoryStream()) {
                CompressionMethod compressionMethod = CompressionMethod.Gzip;
                const int rowGroupSize = 5000;
                ParquetSchema outputSchema = await ParquetConvert.SerializeAsync(items, ms, schema, compressionMethod, rowGroupSize, false)
                    .ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task Serialize_explicit_schema_dataFields_with_null_propertyNames() {
            // unit test for https://github.com/aloneguid/parquet-dotnet/issues/179

            // create items to be serialised
            List<SimpleWithDateTimeAndDecimal> items = new List<SimpleWithDateTimeAndDecimal>() {
                new SimpleWithDateTimeAndDecimal() {
                    DateTimeValue = DateTime.UtcNow,
                    DecimalValue = 123m,
                    Int32Value = 456,
                },
                new SimpleWithDateTimeAndDecimal() {
                    DateTimeValue = DateTime.Now,
                    DecimalValue = 234m,
                    Int32Value = 567,
                },
            };

            // create schema - set DataField.propertyName to null
            ParquetSchema schema = new ParquetSchema(
                new DateTimeDataField(nameof(SimpleWithDateTimeAndDecimal.DateTimeValue), DateTimeFormat.Date, false, null),
                new DecimalDataField(nameof(SimpleWithDateTimeAndDecimal.DecimalValue), 38, 18, false, false, null),
                new DataField(nameof(SimpleWithDateTimeAndDecimal.Int32Value), typeof(int))
                );

            // serialise items
            using(MemoryStream ms = new MemoryStream()) {
                CompressionMethod compressionMethod = CompressionMethod.Gzip;
                const int rowGroupSize = 5000;
                ParquetSchema outputSchema = await ParquetConvert.SerializeAsync(items, ms, schema, compressionMethod, rowGroupSize, false)
                    .ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task Serialize_explicit_schema_generic_dataFields() {
            // unit test for https://github.com/aloneguid/parquet-dotnet/issues/179

            // create items to be serialised
            List<SimpleWithDateTimeAndDecimal> items = new List<SimpleWithDateTimeAndDecimal>() {
                new SimpleWithDateTimeAndDecimal() {
                    DateTimeValue = DateTime.UtcNow,
                    DecimalValue = 123m,
                    Int32Value = 456,
                },
                new SimpleWithDateTimeAndDecimal() {
                    DateTimeValue = DateTime.Now,
                    DecimalValue = 234m,
                    Int32Value = 567,
                },
            };

            // create schema - set DataField.propertyName to null
            ParquetSchema schema = new ParquetSchema(
                new DataField<DateTime>(nameof(SimpleWithDateTimeAndDecimal.DateTimeValue)),
                new DataField<decimal>(nameof(SimpleWithDateTimeAndDecimal.DecimalValue)),
                new DataField<int>(nameof(SimpleWithDateTimeAndDecimal.Int32Value))
                );

            // serialise items
            using(MemoryStream ms = new MemoryStream()) {
                CompressionMethod compressionMethod = CompressionMethod.Gzip;
                const int rowGroupSize = 5000;
                ParquetSchema outputSchema = await ParquetConvert.SerializeAsync(items, ms, schema, compressionMethod, rowGroupSize, false)
                    .ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task Serialize_structure_with_DateTime() {
            await TestRoundTripSerialization<DateTime>(DateTime.UtcNow.RoundToSecond());
        }

        [Fact]
        public async Task Serialize_structure_with_nullable_DateTime() {
            await TestRoundTripSerialization<DateTime?>(DateTime.UtcNow.RoundToSecond());
            await TestRoundTripSerialization<DateTime?>(null);
        }

        [Fact(Skip = "not sure where it was introduced, needs investigation")]
        public async Task Serialise_groups() {
            DateTime now = DateTime.Now;

            IEnumerable<SimpleStructure> structures = Enumerable
               .Range(start: 0, count: 10)
               .Select(i => new SimpleStructure {
                   Id = i,
                   NullableId = (i % 2 == 0) ? new int?() : new int?(i),
                   Name = $"row {i}",
                   Date = now.AddDays(i).RoundToSecond().ToUniversalTime()
               });

            using(var ms = new MemoryStream()) {
                ParquetSchema schema = await ParquetConvert.SerializeAsync(structures, ms, compressionMethod: CompressionMethod.Snappy, rowGroupSize: 2);

                ms.Position = 0;

                SimpleStructure[/*Groups*/][] groups2 = (await ParquetConvert.DeserializeGroupsAsync<SimpleStructure>(ms)).ToArray();
                Assert.Equal(10 / 2, groups2.Length); //groups = count/rowGroupSize

                SimpleStructure[] structuresArray = structures.ToArray();

                SimpleStructure[] structures2 = (
                   from g in groups2
                   from s in g
                   select s
                ).ToArray();

                for(int i = 0; i < 10; i++) {
                    Assert.Equal(structuresArray[i].Id, structures2[i].Id);
                    Assert.Equal(structuresArray[i].NullableId, structures2[i].NullableId);
                    Assert.Equal(structuresArray[i].Name, structures2[i].Name);
                    Assert.Equal(structuresArray[i].Date, structures2[i].Date);
                }
            }
        }

        class NullableDateTime {
            public int Id { get; set; }

            public DateTime? DateTime { get; set; }
        }

        [Fact]
        public async Task Serialize_column_with_all_nulls() {

            // test steps.
            var data = Enumerable.Range(0, 10000).Select(i => new NullableDateTime { Id = i, DateTime = DateTime.Now }).ToList();

            foreach(NullableDateTime item in data.Take(7000)) {
                item.DateTime = null;
            }

            var stream = new MemoryStream();

            ParquetSchema schema = await ParquetConvert.SerializeAsync(data, stream);

            try {
                NullableDateTime[] result = await ParquetConvert.DeserializeAsync<NullableDateTime>(stream);
            }
            catch(Exception e) {
                string res = JsonConvert.SerializeObject(e);
                Console.WriteLine(e);
                throw;
            }

        }

        async Task TestRoundTripSerialization<T>(T value) {
            StructureWithTestType<T> input = new StructureWithTestType<T> {
                Id = "1",
                TestValue = value,
            };

            ParquetSchema schema = typeof(StructureWithTestType<T>).GetParquetSchema(true);

            using(MemoryStream stream = new MemoryStream()) {
                await ParquetConvert.SerializeAsync<StructureWithTestType<T>>(new StructureWithTestType<T>[] { input }, stream, schema);

                stream.Position = 0;
                StructureWithTestType<T>[] output = await ParquetConvert.DeserializeAsync<StructureWithTestType<T>>(stream);
                Assert.Single(output);
                Assert.Equal("1", output[0].Id);
                Assert.Equal(value, output[0].TestValue);
            }
        }

        public class SimplestStructure {
            public int Id { get; set; }

            public string? Name { get; set; }
        }

        public class SimpleRepeated {
            public int Id { get; set; }

            public int[]? Areas { get; set; }
        }

        public class SimpleStructure {
            public int Id { get; set; }

            public int? NullableId { get; set; }

            public string? Name { get; set; }

            public DateTime Date { get; set; }
        }

        public class SimpleChangedStructure {
            public int Id { get; set; }

            public int? NullableId { get; set; }

            public string? Name { get; set; }

            public TimeSpan Offset { get; set; }
        }

        public class SimpleStructureWithFewProperties {
            public int Id { get; set; }
            public string? Name { get; set; }
        }
        public class StructureWithIgnoredProperties {
            public int Id { get; set; }
            public string? Name { get; set; }

            [ParquetIgnore]
            public string? SSN { get; set; }

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

        public class SimpleRenamed {
            public int Id { get; set; }

            [ParquetColumn("Name")]
            public string? PersonName { get; set; }

            //Validate Backwards compatibility of default Decimal Precision and Scale values broken in v3.9.
            [ParquetColumn("DecimalColumnRenamed")]
            public decimal? NullableDecimal { get; set; }
        }

        public class SimpleWithListField {
            [ParquetColumn(UseListField = true, ListContainerName = "bag", ListElementName = "array_element")]
            public int[]? col1 { get; set; }

            [ParquetColumn(UseListField = true)]
            public int[]? col2 { get; set; }

            [ParquetColumn]
            public int[]? col3 { get; set; }
        }

        public class SimpleWithDateTimeAndDecimal {
            public DateTime DateTimeValue { get; set; }
            public decimal DecimalValue { get; set; }
            public int Int32Value { get; set; }
        }

        public class StructureWithTestType<T> {
            T? testValue;

            public string? Id { get; set; }

            // public T TestValue { get; set; }
            public T? TestValue { get { return testValue; } set { testValue = value; } }
        }

        public class SimpleMap {

            [JsonPropertyName("t")]
            public DateTime Timestamp { get; set; }

            [JsonPropertyName("ip")]
            public string IpAddress { get; set; }

            [JsonPropertyName("tags")]
            public Dictionary<string, string?> Tags { get; set; }
        }
    }
}