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
         Assert.Throws<NotSupportedException>(() => new DataField<Enum>("e"));
      }

      [Fact]
      public void SchemaElement_are_equal()
      {
         Assert.Equal(new DataField<int>("id"), new DataField<int>("id"));
      }

      [Fact]
      public void SchemaElement_different_names_not_equal()
      {
         Assert.NotEqual(new DataField<int>("id1"), new DataField<int>("id"));
      }

      [Fact]
      public void SchemaElement_different_types_not_equal()
      {
         Assert.NotEqual((Field)(new DataField<int>("id")), (Field)(new DataField<double>("id")));
      }

      [Fact]
      public void Schemas_idential_equal()
      {
         var schema1 = new Schema(new DataField<int>("id"), new DataField<string>("city"));
         var schema2 = new Schema(new DataField<int>("id"), new DataField<string>("city"));

         Assert.Equal(schema1, schema2);
      }

      [Fact]
      public void Schemas_different_not_equal()
      {
         var schema1 = new Schema(new DataField<int>("id"), new DataField<string>("city"));
         var schema2 = new Schema(new DataField<int>("id"), new DataField<string>("city2"));

         Assert.NotEqual(schema1, schema2);
      }

      [Fact]
      public void Schemas_differ_only_in_repeated_fields_not_equal()
      {
         var schema1 = new Schema(new DataField<int>("id"), new DataField<string>("cities"));
         var schema2 = new Schema(new DataField<int>("id"), new DataField<IEnumerable<string>>("cities"));

         Assert.NotEqual(schema1, schema2);
      }

      [Fact]
      public void Schemas_differ_by_nullability()
      {
         Assert.NotEqual<Field>(
            new DataField<int>("id"),
            new DataField<int?>("id"));
      }

      [Fact]
      public void String_is_always_nullable()
      {
         var se = new DataField<string>("id");

         Assert.True(se.HasNulls);
      }

      [Fact]
      public void Datetime_is_not_nullable_by_default()
      {
         var se = new DataField<DateTime>("id");

         Assert.False(se.HasNulls);
      }

      [Fact]
      public void Generic_dictionaries_are_not_allowed()
      {
         Assert.Throws<NotSupportedException>(() => new DataField<IDictionary<int, string>>("dictionary"));
      }

      [Fact]
      public void Invalid_dictionary_declaration()
      {
         Assert.Throws<ArgumentException>(() => new DataField<Dictionary<int, int>>("d"));
      }

      [Fact]
      public void But_i_can_declare_a_dictionary()
      {
         new MapField("dictionary", new DataField("key", DataType.Int32), new DataField("value", DataType.String));
      }

      [Fact]
      public void Map_fields_with_same_types_are_equal()
      {
         Assert.Equal(new MapField("dictionary", new DataField("key", DataType.Int32), new DataField("value", DataType.String)),
                      new MapField("dictionary", new DataField("key", DataType.Int32), new DataField("value", DataType.String)));

      }

      [Fact]
      public void Map_fields_with_different_types_are_unequal()
      {
         Assert.NotEqual(new MapField("dictionary", new DataField("key", DataType.String), new DataField("value", DataType.String)),
                      new MapField("dictionary", new DataField("key", DataType.Int32), new DataField("value", DataType.String)));

      }

      [Fact]
      public void Byte_array_schema_is_a_bytearray_type()
      {
         var field = new DataField<byte[]>("bytes");
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
         Assert.Throws<ArgumentException>(() => new DataField<Row>("struct"));
      }

      [Fact]
      public void Cannot_use_dots_in_field_names()
      {
         Assert.Throws<ArgumentException>(() => new DataField<int>("one.two"));
      }

      [Fact]
      public void Identical_structs_field_are_equal()
      {
         Assert.Equal(
            new StructField("f1", new DataField<string>("name")),
            new StructField("f1", new DataField<string>("name")));
      }

      [Fact]
      public void Structs_named_different_are_not_equal()
      {
         Assert.NotEqual(
            new StructField("f1", new DataField<string>("name")),
            new StructField("f2", new DataField<string>("name")));
      }

      [Fact]
      public void Structs_with_different_children_are_not_equal()
      {
         Assert.NotEqual(
            new StructField("f1", new DataField<string>("name")),
            new StructField("f1", new DataField<int>("name")));
      }

      [Fact]
      public void Lists_are_equal()
      {
         Assert.Equal(
            new ListField("item", new DataField<int>("id")),
            new ListField("item", new DataField<int>("id"))
         );
      }

      [Fact]
      public void Lists_are_not_equal_by_name()
      {
         Assert.NotEqual(
            new ListField("item", new DataField<int>("id")),
            new ListField("item1", new DataField<int>("id"))
         );
      }

      [Fact]
      public void Lists_are_not_equal_by_item()
      {
         Assert.NotEqual(
            new ListField("item", new DataField<int>("id")),
            new ListField("item", new DataField<string>("id"))
         );
      }

      [Fact]
      public void List_maintains_path_prefix()
      {
         var list = new ListField("List", new DataField<int>("id"));
         list.PathPrefix = "Parent";

         Assert.Equal("Parent.List.list.id", list.Item.Path);
      }
   }
}
