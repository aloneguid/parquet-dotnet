using Parquet.Data;
using System;
using Xunit;

namespace Parquet.Test
{
   public class DataSetTest
   {
      [Fact]
      public void Validate_cant_add_null_row()
      {
         var ds = new DataSet();
         Assert.Throws<ArgumentNullException>(() => ds.Add((Row)null));
      }

      [Fact]
      public void Validate_type_mismatch_throws_exception()
      {
         var ds = new DataSet(new SchemaElement<string>("s"));

         Assert.Throws<ArgumentException>(() => ds.Add(4));
      }

      [Fact]
      public void Validate_wrongcolnumber_throws_exception()
      {
         var ds = new DataSet(new SchemaElement<string>("s"));

         Assert.Throws<ArgumentException>(() => ds.Add("1", 2));
      }
   }
}
