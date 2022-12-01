// TODO: verify these are all correct/required once i'm back on internet
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using Parquet;
using Parquet.Data;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test {
  public class InheritedPropertiesTest : TestBase {
    [Fact]
    public async Task Serialize_class_with_inherited_properties() {
      InheritedClass expected = new() {
        BasePropertyA = "A",
        InheritedPropertyB = "B"
      };

      InheritedClass[] recordsToSerialize = new InheritedClass[1]
      {
        expected
      };

      Schema schema = SchemaReflector.ReflectWithInheritedProperties<InheritedClass>();

      // TODO: is there some `with/using` syntax i should use here?
      MemoryStream stream = new();
      await ParquetConvert.SerializeAsync(recordsToSerialize, stream, schema);
      InheritedClass[] deserializedRecords = await ParquetConvert.DeserializeAsync<InheritedClass>(stream);
      
      InheritedClass actual = deserializedRecords[0];

      // TODO: should we instead assert that 
      //    `deserialized[0].BaseProperty is not null`?
      Assert.Equal(expected, actual);
    }

    private class BaseClass {
      public string BasePropertyA { get; set; }
    }

    private class InheritedClass : BaseClass {
      public string InheritedPropertyB { get; set; }
    }
  }
}
