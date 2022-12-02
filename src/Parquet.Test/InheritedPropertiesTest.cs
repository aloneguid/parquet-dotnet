// TODO: verify these are all correct/required once i'm back on internet
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System.IO;
using Parquet;
using Parquet.Data;
using Parquet.Serialization;
using Parquet.Serialization.Values;
using Xunit;

namespace Parquet.Test {
  public class InheritedPropertiesTest : TestBase {
    private InheritedClass[] GenerateRecordsToSerialize() {
      InheritedClass record = new() {
        BaseProperty = "A",
        InheritedProperty = "B"
      };

      InheritedClass[] recordsToSerialize = new InheritedClass[1]
      {
        record
      };

      return recordsToSerialize;
    }

    [Fact]
    public async Task Serialize_class_with_inherited_properties() {
      InheritedClass[] recordsToSerialize = GenerateRecordsToSerialize();
      Schema schema = SchemaReflector.ReflectWithInheritedProperties<InheritedClass>();

      // TODO: is there some `with/using` syntax i should use here?
      MemoryStream stream = new();
      await ParquetConvert.SerializeAsync(recordsToSerialize, stream, schema);
      InheritedClass[] deserializedRecords = await ParquetConvert.DeserializeAsync<InheritedClass>(stream, fileSchema: schema);
      
      InheritedClass expected = recordsToSerialize[0];
      InheritedClass actual = deserializedRecords[0];

      // TODO: should we instead assert that 
      //    `deserialized[0].BaseProperty is not null`?
      Assert.Equal(expected.BaseProperty, actual.BaseProperty);
    }

    [Fact] 
    public async Task Writer_should_have_inherited_columns() {
      InheritedClass[] records = GenerateRecordsToSerialize();
      Schema schema = SchemaReflector.ReflectWithInheritedProperties<InheritedClass>();
      DataField[] dataFields = schema.GetDataFields();
      ClrBridge bridge = new(typeof(InheritedClass));
      DataColumn[] columns = dataFields
          .Select(df => bridge.BuildColumn(df, records, 1))
          .ToArray();

      // TODO: fix this assertion to be more specific
      Assert.Equal(columns.Length, 2);
    }

    private class BaseClass {
      public string BaseProperty { get; set; }
    }

    private class InheritedClass : BaseClass {
      public string InheritedProperty { get; set; }
    }
  }
}
