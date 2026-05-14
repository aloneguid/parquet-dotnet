namespace Parquet.Test.Serialisation;

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