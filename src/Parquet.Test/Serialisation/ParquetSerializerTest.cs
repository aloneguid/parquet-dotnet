﻿using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test.Serialisation {
    public class ParquetSerializerTest {

        class Record : IEquatable<Record> {
            public DateTime Timestamp { get; set; }
            public string? EventName { get; set; }
            public double MeterValue { get; set; }

            public bool Equals(Record? other) {
                if(other == null)
                    return false;

                return Timestamp == other.Timestamp &&
                    EventName == other.EventName &&
                    MeterValue == other.MeterValue;
            }
        }

        [Fact]
        public async Task SerializeDeserializeRecord() {

            var data = Enumerable.Range(0, 1_000_000).Select(i => new Record {
                Timestamp = DateTime.UtcNow.AddSeconds(i),
                EventName = i % 2 == 0 ? "on" : "off",
                MeterValue = i
            }).ToList();

            using var ms = new MemoryStream();
            await ParquetSerializer.SerializeAsync(data, ms);

            ms.Position = 0;
            IList<Record> data2 = await ParquetSerializer.DeserializeAsync<Record>(ms);

            Assert.Equal(data2, data);
        }

        class Address : IEquatable<Address> {
            public string? Country { get; set; }

            public string? City { get; set; }

            public bool Equals(Address? other) =>
                Country == other?.Country &&
                City == other?.City;
        }

        class AddressBookEntry : IEquatable<AddressBookEntry> {
            public string? FirstName { get; set; }

            public string? LastName { get; set; }   

            public Address? Address { get; set; }

            public bool Equals(AddressBookEntry? other) {
                return FirstName == other?.FirstName &&
                    LastName == other?.LastName &&
                    ((Address == null && other?.Address == null) || (Address != null && Address.Equals(other?.Address)));
            }
        }

        [Fact]
        public async Task SerializeDeserializeStruct() {

            var data = Enumerable.Range(0, 1_000_000).Select(i => new AddressBookEntry {
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

            Assert.Equal(data2, data);
        }

    }
}