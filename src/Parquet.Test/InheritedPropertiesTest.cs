using System.Threading.Tasks;
using System.IO;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test {
    public class InheritedPropertiesTest : TestBase {
        private InheritedClass[] GenerateRecordsToSerialize() {
            InheritedClass record = new() {
                BaseProperty = "A",
                InheritedProperty = "B"
            };

            InheritedClass[] recordsToSerialize = new InheritedClass[1] {
                record
            };

            return recordsToSerialize;
        }

        [Fact]
        public async Task Serialize_class_without_inherited_properties() {
            InheritedClass[] recordsToSerialize = GenerateRecordsToSerialize();
            ParquetSchema schema = SchemaReflector.Reflect<InheritedClass>();

            // TODO: is there some `with/using` syntax i should use here?
            MemoryStream stream = new();
            await ParquetConvert.SerializeAsync(recordsToSerialize, stream, schema);
            InheritedClass[] deserializedRecords =
                await ParquetConvert.DeserializeAsync<InheritedClass>(stream, fileSchema: schema);

            InheritedClass expected = recordsToSerialize[0];
            InheritedClass actual = deserializedRecords[0];

            Assert.Null(actual.BaseProperty);
            Assert.Equal(expected.InheritedProperty, actual.InheritedProperty);
        }

        [Fact]
        public async Task Serialize_class_with_inherited_properties() {
            InheritedClass[] recordsToSerialize = GenerateRecordsToSerialize();
            ParquetSchema schema = SchemaReflector.ReflectWithInheritedProperties<InheritedClass>();

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
        }

        private class BaseClass {
            public string? BaseProperty { get; set; }
        }

        private class InheritedClass : BaseClass {
            public string? InheritedProperty { get; set; }
        }
    }
}