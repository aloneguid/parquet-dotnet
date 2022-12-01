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
      InheritedClass inheritedClass = new() {
        BasePropertyA = "A",
        InheritedPropertyB = "B"
      };

      InheritedClass[] records = new InheritedClass[1]
      {
        inheritedClass
      };

      // serialize and deserialize and expect them to be the same; check serialized for base property
      MemoryStream stream = new();
      await ParquetConvert.SerializeAsync(records, stream);
      InheritedClass[] deserializedRecords = await ParquetConvert.DeserializeAsync<InheritedClass>(stream);
      
      Assert.Equal(deserializedRecords[0], records[0]);
    }

    private class BaseClass {
      public string BasePropertyA { get; set; }
    }

    private class InheritedClass : BaseClass {
      public string InheritedPropertyB { get; set; }
    }
  }
}
