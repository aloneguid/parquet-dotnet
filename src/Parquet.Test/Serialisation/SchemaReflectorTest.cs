using Parquet.Attributes;
using Parquet.Data;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test.Serialisation
{
   public class SchemaReflectorTest : TestBase
   {
      [Fact]
      public void I_can_infer_different_types()
      {
         var inferrer = new SchemaReflector(typeof(PocoClass));

         Schema schema = inferrer.Reflect();

         Assert.NotNull(schema);
         Assert.Equal(4, schema.Fields.Count);

         VerifyPocoClassFields(schema);
      }

      [Fact]
      public void I_ignore_inherited_properties()
      {
         Schema schema = SchemaReflector.Reflect<PocoSubClass>();
         Assert.Equal(1, schema.Fields.Count);
         VerifyPocoSubClassField((DataField)schema[0]);
      }

      [Fact]
      public void I_can_recognize_inherited_properties() 
      {
         Schema schema = SchemaReflector.ReflectWithInheritedProperties<PocoSubClass>();
         Assert.Equal(5, schema.Fields.Count);
         VerifyPocoClassFields(schema);
         VerifyPocoSubClassField((DataField) schema[4]);
      }

      private static void VerifyPocoClassFields(Schema schema) {
         DataField id = (DataField)schema[0];
         Assert.Equal("Id", id.Name);
         Assert.Equal(DataType.Int32, id.DataType);
         Assert.False(id.HasNulls);
         Assert.False(id.IsArray);

         DataField altId = (DataField)schema[1];
         Assert.Equal("AltId", altId.Name);
         Assert.Equal(DataType.Int32, id.DataType);
         Assert.False(id.HasNulls);
         Assert.False(id.IsArray);

         DataField nullableFloat = (DataField)schema[2];
         Assert.Equal("NullableFloat", nullableFloat.Name);
         Assert.Equal(DataType.Float, nullableFloat.DataType);
         Assert.True(nullableFloat.HasNulls);
         Assert.False(nullableFloat.IsArray);

         DataField intArray = (DataField)schema[3];
         Assert.Equal("IntArray", intArray.Name);
         Assert.Equal(DataType.Int32, intArray.DataType);
         Assert.False(intArray.HasNulls);
         Assert.True(intArray.IsArray);
      }

      private static void VerifyPocoSubClassField(DataField extraProp) {
         Assert.Equal("ExtraProperty", extraProp.Name);
         Assert.Equal(DataType.Int32, extraProp.DataType);
         Assert.False(extraProp.HasNulls);
         Assert.False(extraProp.IsArray);
      }

      /// <summary>
      /// Essentially all the test cases are this class' fields
      /// </summary>
      class PocoClass
      {
         public int Id { get; set; }

         [ParquetColumn("AltId")]
         public int AnnotatedId { get; set; }

         public float? NullableFloat { get; set; }

         public int[] IntArray { get; set; }
      }

      class PocoSubClass : PocoClass
      {
         public int ExtraProperty { get; set; }
      }
   }
}
