using System;
using Parquet.Schema;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test.Serialisation {
    public class SchemaInferrerTest {


        [Fact]
        public void InferPrimitiveTypes() {
            ParquetSchema schema = new SchemaInferrer<ConvenientTypes>().CreateSchema(false);

            Assert.Equal(new ParquetSchema(
                new DataField<int>("Int"),
                new DataField<int>("GettableInt"),
                new DataField<int?>("NullableInt")
                ), schema);
        }

        [Fact]
        public void InferStruct() {
            ParquetSchema schema = new SchemaInferrer<StructMaster>().CreateSchema(false);

            Assert.Equal(new ParquetSchema(
                new DataField<string>("Version"),
                new StructField("One",
                    new DataField<int>("Int"))
                ), schema);
        }

        public class StructMemberOne {
            public int Int { get; set; }
        }

        public class StructMaster {

            public string? Version { get; set; }

            public StructMemberOne One { get; set; } = new StructMemberOne();
        }

        public class PrimitiveTypes {
            public int Int { get; set; }

            public int GettableInt { get; }

            public int? NullableInt { get; set; }
        }

        public class ConvenientTypes : PrimitiveTypes {
            [ParquetIgnore]
            public Guid Guid { get; set; }
        }

    }


}
