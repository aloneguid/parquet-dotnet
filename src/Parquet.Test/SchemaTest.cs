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
         Assert.Throws<NotSupportedException>(() => new SchemaElement<Enum>("e"));
      }

      [Fact]
      public void Creating_schema_with_nullable_primitive_fails()
      {
         Assert.Throws<ArgumentException>(() => new SchemaElement<int?>("fail!"));
      }

      [Fact]
      public void SchemaElement_are_equal()
      {
         Assert.Equal(new SchemaElement<int>("id"), new SchemaElement<int>("id"));
      }

      [Fact]
      public void SchemaElement_different_names_not_equal()
      {
         Assert.NotEqual(new SchemaElement<int>("id1"), new SchemaElement<int>("id"));
      }

      [Fact]
      public void SchemaElement_different_types_not_equal()
      {
         Assert.NotEqual((SchemaElement)(new SchemaElement<int>("id")), (SchemaElement)(new SchemaElement<double>("id")));
      }

      [Fact]
      public void Schemas_idential_equal()
      {
         var schema1 = new Schema(new SchemaElement<int>("id"), new SchemaElement<string>("city"));
         var schema2 = new Schema(new SchemaElement<int>("id"), new SchemaElement<string>("city"));

         Assert.Equal(schema1, schema2);
      }

      [Fact]
      public void Schemas_different_not_equal()
      {
         var schema1 = new Schema(new SchemaElement<int>("id"), new SchemaElement<string>("city"));
         var schema2 = new Schema(new SchemaElement<int>("id"), new SchemaElement<string>("city2"));

         Assert.NotEqual(schema1, schema2);
      }

      [Fact]
      public void Schemas_differ_only_in_repeated_fields_not_equal()
      {
         var schema1 = new Schema(new SchemaElement<int>("id"), new SchemaElement<string>("cities"));
         var schema2 = new Schema(new SchemaElement<int>("id"), new SchemaElement<IEnumerable<string>>("cities"));

         Assert.NotEqual(schema1, schema2);
      }
   }
}
