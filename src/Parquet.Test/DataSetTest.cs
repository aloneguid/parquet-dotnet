using Parquet.Data;
using System;
using Xunit;
using System.Collections.Generic;

namespace Parquet.Test
{
   public class DataSetTest
   {
      [Fact]
      public void Validate_cant_add_null_row()
      {
         var ds = new DataSet(new Field<int>("id"));
         Assert.Throws<ArgumentNullException>(() => ds.Add((Row)null));
      }

      [Fact]
      public void Validate_type_mismatch_throws_exception()
      {
         var ds = new DataSet(new Field<string>("s"));

         Assert.Throws<ArgumentException>(() => ds.Add(4));
      }

      [Fact]
      public void Validate_wrongcolnumber_throws_exception()
      {
         var ds = new DataSet(new Field<string>("s"));

         Assert.Throws<ArgumentException>(() => ds.Add("1", 2));
      }

      [Fact]
      public void Can_add_repeatable_fields()
      {
         var ds = new DataSet(
            new Field<int>("id"),
            new Field<IEnumerable<string>>("rep"));

         ds.Add(1, new[] { "one", "two" });
      }
   }
}
