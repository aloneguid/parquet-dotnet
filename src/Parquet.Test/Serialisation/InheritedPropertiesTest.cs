using System.Threading.Tasks;
using System.IO;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Serialization;
using Xunit;
using System;

namespace Parquet.Test.Serialisation {
    public class InheritedPropertiesTest : TestBase {
        private InheritedClass[] GenerateRecordsToSerialize() {
            InheritedClass record = new() {
                BasePropertyLevel2 = "0",
                BaseProperty = "A",
                InheritedProperty = "B"
            };

            var recordsToSerialize = new InheritedClass[1] {
                record
            };

            return recordsToSerialize;
        }

        [Obsolete, Fact]
        public async Task Serialize_class_with_inherited_properties() {
            InheritedClass[] recordsToSerialize = GenerateRecordsToSerialize();
            ParquetSchema schema = typeof(InheritedClass).GetParquetSchema(true);

            // TODO: is there some `with/using` syntax i should use here?
            MemoryStream stream = new();
            await ParquetConvert.SerializeAsync(recordsToSerialize, stream, schema);
            InheritedClass[] deserializedRecords =
                await ParquetConvert.DeserializeAsync<InheritedClass>(stream, fileSchema: schema);

            InheritedClass expected = recordsToSerialize[0];
            InheritedClass actual = deserializedRecords[0];

            Assert.Equal(expected.InheritedProperty, actual.InheritedProperty);
            Assert.NotNull(actual.BaseProperty);
            Assert.Equal(expected.BaseProperty, actual.BaseProperty);
            Assert.NotNull(actual.BasePropertyLevel2);
            Assert.Equal(expected.BasePropertyLevel2, actual.BasePropertyLevel2);
        }

        private class BaseClassLevel2 {
            public string? BasePropertyLevel2 { get; set; }
        }

        private class BaseClass : BaseClassLevel2 {
            public string? BaseProperty { get; set; }
        }

        private class InheritedClass : BaseClass {
            public string? InheritedProperty { get; set; }
        }
    }
}