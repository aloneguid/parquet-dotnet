using Parquet.Schema;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test.Serialisation {
    public class SchemaReflectorTest : TestBase {
        [Fact]
        public void I_can_infer_different_types() {
            var inferrer = new SchemaReflector(typeof(PocoClass));

            ParquetSchema schema = inferrer.Reflect();

            Assert.NotNull(schema);
            Assert.Equal(4, schema.Fields.Count);

            VerifyPocoClassFields(schema);
        }

        [Fact]
        public void I_ignore_inherited_properties() {
            ParquetSchema schema = SchemaReflector.Reflect<PocoSubClass>();
            Assert.Equal(1, schema.Fields.Count);
            VerifyPocoSubClassField((DataField)schema[0]);
        }

        [Fact]
        public void I_can_recognize_inherited_properties() {
            ParquetSchema schema = SchemaReflector.ReflectWithInheritedProperties<PocoSubClass>();
            Assert.Equal(5, schema.Fields.Count);
            VerifyPocoClassFields(schema);
            VerifyPocoSubClassField((DataField)schema[4]);
        }

        [Fact]
        public void Reflecting_with_inherited_properties_after_default_does_not_cause_cache_collisions() {
            ParquetSchema defaultSchema = SchemaReflector.Reflect<PocoSubClass>();
            ParquetSchema schemaWithInheritedProps = SchemaReflector.ReflectWithInheritedProperties<PocoSubClass>();

            Assert.Equal(1, defaultSchema.Fields.Count);
            Assert.Equal(5, schemaWithInheritedProps.Fields.Count);
            VerifyPocoSubClassField((DataField)defaultSchema[0]);
            VerifyPocoClassFields(schemaWithInheritedProps);
        }

        [Fact]
        public void Reflecting_with_inherited_properties_before_default_does_not_cause_cache_collisions() {
            ParquetSchema schemaWithInheritedProps = SchemaReflector.ReflectWithInheritedProperties<PocoSubClass>();
            ParquetSchema defaultSchema = SchemaReflector.Reflect<PocoSubClass>();

            Assert.Equal(1, defaultSchema.Fields.Count);
            Assert.Equal(5, schemaWithInheritedProps.Fields.Count);
            VerifyPocoSubClassField((DataField)defaultSchema[0]);
            VerifyPocoClassFields(schemaWithInheritedProps);
        }

        private static void VerifyPocoClassFields(ParquetSchema schema) {
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

            public int[] IntArray { get; set; }
        }

        class PocoSubClass : PocoClass {
            public int ExtraProperty { get; set; }
        }
    }
}