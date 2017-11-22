using Parquet.Data;
using System;
using Xunit;
using System.Collections.Generic;

namespace Parquet.Test
{
   public class SchemaTest
   {
      [Fact]
      public void Creating_element_with_unsupported_type_throws_exception()
      {
         Assert.Throws<NotSupportedException>(() => new Field<Enum>("e"));
      }

      [Fact]
      public void SchemaElement_are_equal()
      {
         Assert.Equal(new Field<int>("id"), new Field<int>("id"));
      }

      [Fact]
      public void SchemaElement_different_names_not_equal()
      {
         Assert.NotEqual(new Field<int>("id1"), new Field<int>("id"));
      }

      [Fact]
      public void SchemaElement_different_types_not_equal()
      {
         Assert.NotEqual((Field)(new Field<int>("id")), (Field)(new Field<double>("id")));
      }

      [Fact]
      public void Schemas_idential_equal()
      {
         var schema1 = new Schema(new Field<int>("id"), new Field<string>("city"));
         var schema2 = new Schema(new Field<int>("id"), new Field<string>("city"));

         Assert.Equal(schema1, schema2);
      }

      [Fact]
      public void Schemas_different_not_equal()
      {
         var schema1 = new Schema(new Field<int>("id"), new Field<string>("city"));
         var schema2 = new Schema(new Field<int>("id"), new Field<string>("city2"));

         Assert.NotEqual(schema1, schema2);
      }

      [Fact]
      public void Schemas_differ_only_in_repeated_fields_not_equal()
      {
         var schema1 = new Schema(new Field<int>("id"), new Field<string>("cities"));
         var schema2 = new Schema(new Field<int>("id"), new Field<IEnumerable<string>>("cities"));

         Assert.NotEqual(schema1, schema2);
      }

      [Fact]
      public void Schemas_differ_by_nullability()
      {
         Assert.NotEqual<Field>(
            new Field<int>("id"),
            new Field<int?>("id"));
      }

      [Fact]
      public void String_is_always_nullable()
      {
         var se = new Field<string>("id");

         Assert.True(se.HasNulls);
      }

      [Fact]
      public void Datetime_is_not_nullable_by_default()
      {
         var se = new Field<DateTime>("id");

         Assert.False(se.HasNulls);
      }

      [Fact]
      public void Generic_dictionaries_are_not_allowed()
      {
         Assert.Throws<NotSupportedException>(() => new Field<IDictionary<int, string>>("dictionary"));
      }

      [Fact]
      public void Invalid_dictionary_declaration()
      {
         Assert.Throws<ArgumentException>(() => new Field<Dictionary<int, int>>("d"));
      }

      [Fact]
      public void But_i_can_declare_a_dictionary()
      {
         new MapField("dictionary", new DataField("key", DataType.Int32), new DataField("value", DataType.String));
      }

      [Fact]
      public void Byte_array_schema_is_a_bytearray_type()
      {
         var field = new Field<byte[]>("bytes");
         Assert.Equal(DataType.ByteArray, field.DataType);
         Assert.Equal(typeof(byte[]), field.ClrType);
         Assert.False(field.IsArray);
         Assert.True(field.HasNulls);
      }

      [Fact]
      public void Cannot_create_struct_with_no_elements()
      {
         Assert.Throws<ArgumentException>(() => new StructField("struct"));
      }

      [Fact]
      public void Generic_field_of_row_throws_a_hint_to_structure()
      {
         Assert.Throws<ArgumentException>(() => new Field<Row>("struct"));
      }
   }
}
