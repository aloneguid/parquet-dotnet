using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using Parquet;
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

      // TODO: is there some `with/using` syntax i should use here?
      MemoryStream stream = new();
      await ParquetConvert.SerializeAsync(recordsToSerialize, stream);
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
