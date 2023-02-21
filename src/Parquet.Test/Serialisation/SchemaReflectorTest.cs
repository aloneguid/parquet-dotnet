using Parquet.Schema;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test.Serialisation {
    public class SchemaReflectorTest : TestBase {
        [Fact]
        public void I_can_infer_different_types() {
            ParquetSchema schema = typeof(PocoClass).GetParquetSchema(true);

            Assert.NotNull(schema);
            Assert.Equal(4, schema.Fields.Count);

            // verify

            DataField id = (DataField)schema[0];
            Assert.Equal("Id", id.Name);
            Assert.Equal(typeof(int), id.ClrType);
            Assert.False(id.IsNullable);
            Assert.False(id.IsArray);

            DataField altId = (DataField)schema[1];
            Assert.Equal("AltId", altId.Name);
            Assert.Equal(typeof(int), id.ClrType);
            Assert.False(id.IsNullable);
            Assert.False(id.IsArray);

            DataField nullableFloat = (DataField)schema[2];
            Assert.Equal("NullableFloat", nullableFloat.Name);
            Assert.Equal(typeof(float), nullableFloat.ClrType);
            Assert.True(nullableFloat.IsNullable);
            Assert.False(nullableFloat.IsArray);

            DataField intArray = (DataField)schema[3];
            Assert.Equal("IntArray", intArray.Name);
            Assert.Equal(typeof(int), intArray.ClrType);
            Assert.False(intArray.IsNullable);
            Assert.True(intArray.IsArray);
        }

        [Fact]
        public void I_can_recognize_inherited_properties() {
            ParquetSchema schema = typeof(PocoSubClass).GetParquetSchema(true);
            Assert.Equal(5, schema.Fields.Count);
            VerifyPocoClassWithInheritedFields(schema);
            VerifyPocoSubClassField((DataField)schema[0]);
        }

        private static void VerifyPocoClassWithInheritedFields(ParquetSchema schema) {
            DataField extraProperty = (DataField)schema[0];
            Assert.Equal("ExtraProperty", extraProperty.Name);
            Assert.Equal(typeof(int), extraProperty.ClrType);
            Assert.False(extraProperty.IsNullable);
            Assert.False(extraProperty.IsArray);

            DataField id = (DataField)schema[1];
            Assert.Equal("Id", id.Name);
            Assert.Equal(typeof(int), id.ClrType);
            Assert.False(id.IsNullable);
            Assert.False(id.IsArray);

            DataField altId = (DataField)schema[2];
            Assert.Equal("AltId", altId.Name);
            Assert.Equal(typeof(int), id.ClrType);
            Assert.False(id.IsNullable);
            Assert.False(id.IsArray);

            DataField nullableFloat = (DataField)schema[3];
            Assert.Equal("NullableFloat", nullableFloat.Name);
            Assert.Equal(typeof(float), nullableFloat.ClrType);
            Assert.True(nullableFloat.IsNullable);
            Assert.False(nullableFloat.IsArray);

            DataField intArray = (DataField)schema[4];
            Assert.Equal("IntArray", intArray.Name);
            Assert.Equal(typeof(int), intArray.ClrType);
            Assert.False(intArray.IsNullable);
            Assert.True(intArray.IsArray);
        }

        private static void VerifyPocoSubClassField(DataField extraProp) {
            Assert.Equal("ExtraProperty", extraProp.Name);
            Assert.Equal(typeof(int), extraProp.ClrType);
            Assert.False(extraProp.IsNullable);
            Assert.False(extraProp.IsArray);
        }

        /// <summary>
        /// Essentially all the test cases are this class' fields
        /// </summary>
        class PocoClass {
            public int Id { get; set; }

            [ParquetColumn("AltId")] public int AnnotatedId { get; set; }
            public float? NullableFloat { get; set; }

            public int[]? IntArray { get; set; }
        }

        class PocoSubClass : PocoClass {
            public int ExtraProperty { get; set; }
        }
    }
}