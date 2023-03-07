using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test.Serialisation {
    public class ParquetSerializerTest {

        class Record {
            public DateTime Timestamp { get; set; }
            public string? EventName { get; set; }
            public double MeterValue { get; set; }
        }

        [Fact]
        public async Task Atomics_Simplest_Serde() {

            var data = Enumerable.Range(0, 1_000).Select(i => new Record {
                Timestamp = DateTime.UtcNow.AddSeconds(i),
                EventName = i % 2 == 0 ? "on" : "off",
                MeterValue = i
            }).ToList();

            using var ms = new MemoryStream();
            await ParquetSerializer.SerializeAsync(data, ms);

            ms.Position = 0;
            IList<Record> data2 = await ParquetSerializer.DeserializeAsync<Record>(ms);

            Assert.Equivalent(data2, data);
        }

        class NullableRecord : Record {
            public int? ParentId { get; set; }
        }

        [Fact]
        public async Task Atomics_Nullable_Serde() {

            var data = Enumerable.Range(0, 1_000).Select(i => new NullableRecord {
                Timestamp = DateTime.UtcNow.AddSeconds(i),
                EventName = i % 2 == 0 ? "on" : "off",
                MeterValue = i,
                ParentId = (i % 4 == 0) ? null : i
            }).ToList();

            using var ms = new MemoryStream();
            await ParquetSerializer.SerializeAsync(data, ms);

            ms.Position = 0;
            IList<NullableRecord> data2 = await ParquetSerializer.DeserializeAsync<NullableRecord>(ms);

            Assert.Equivalent(data2, data);
        }

        class Address {
            public string? Country { get; set; }

            public string? City { get; set; }
        }

        class AddressBookEntry { 
            public string? FirstName { get; set; }

            public string? LastName { get; set; }   

            public Address? Address { get; set; }
        }

        [Fact]
        public async Task Struct_Serde() {

            var data = Enumerable.Range(0, 1_000).Select(i => new AddressBookEntry {
                FirstName = "Joe",
                LastName = "Bloggs",
                Address = new Address() {
                    Country = "UK",
                    City = "Unknown"
                }
            }).ToList();

            using var ms = new MemoryStream();
            await ParquetSerializer.SerializeAsync(data, ms);

            ms.Position = 0;
            IList<AddressBookEntry> data2 = await ParquetSerializer.DeserializeAsync<AddressBookEntry>(ms);

            Assert.Equivalent(data2, data);
        }

        class MovementHistory {
            public int? PersonId { get; set; }

            public string? Comments { get; set; }

            public List<Address>? Addresses { get; set; }
        }

        [Fact]
        public async Task Struct_WithNullProps_Serde() {

            var data = Enumerable.Range(0, 1_000).Select(i => new AddressBookEntry {
                FirstName = "Joe",
                LastName = "Bloggs"
                // Address is null
            }).ToList();

            using var ms = new MemoryStream();
            await ParquetSerializer.SerializeAsync(data, ms);

            ms.Position = 0;
            IList<AddressBookEntry> data2 = await ParquetSerializer.DeserializeAsync<AddressBookEntry>(ms);

            Assert.Equivalent(data2, data);
        }

        [Fact]
        public async Task List_Structs_Serde() {
            var data = Enumerable.Range(0, 1_000).Select(i => new MovementHistory {
                PersonId = i,
                Comments = i % 2 == 0 ? "none" : null,
                Addresses = Enumerable.Range(0, 4).Select(a => new Address {
                    City = "Birmingham",
                    Country = "United Kingdom"
                }).ToList()
            }).ToList();

            // serialise
            using var ms = new MemoryStream();
            await ParquetSerializer.SerializeAsync(data, ms);

            // deserialise
            ms.Position = 0;
            IList<MovementHistory> data2 = await ParquetSerializer.DeserializeAsync<MovementHistory>(ms);

            // assert
            Assert.Equivalent(data, data2);

        }

        class MovementHistoryCompressed : IEquatable<MovementHistoryCompressed> {
            public int? PersonId { get; set; }

            public List<int>? ParentIds { get; set; }

            public bool Equals(MovementHistoryCompressed? other) {
                if(other == null)
                    return false;

                if(ParentIds?.Count != other?.ParentIds?.Count)
                    return false;

                if(ParentIds != null && other?.ParentIds != null) {
                    for(int i = 0; i < ParentIds.Count; i++) {
                        if(ParentIds[i] != other.ParentIds[i]) return false;
                    }
                }

                return true;
            }
        }

        [Fact]
        public async Task List_Atomics_Serde() {

            var data = Enumerable.Range(0, 100).Select(i => new MovementHistoryCompressed {
                PersonId = i,
                ParentIds = Enumerable.Range(i, 4).ToList()
            }).ToList();

            // serialise
            using var ms = new MemoryStream();
            await ParquetSerializer.SerializeAsync(data, ms);

            // deserialise
            ms.Position = 0;
            IList<MovementHistoryCompressed> data2 = await ParquetSerializer.DeserializeAsync<MovementHistoryCompressed>(ms);

            // assert
            Assert.Equivalent(data, data2);

        }
    }
}
