using Parquet.Data;
using System;
using Xunit;

namespace Parquet.Test
{
   public class SchemaElementTest
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
   }
}
