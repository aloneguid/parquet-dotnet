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
         var ds = new DataSet(new DataField<int>("id"));
         Assert.Throws<ArgumentNullException>(() => ds.Add((Row)null));
      }

      [Fact]
      public void Validate_type_mismatch_throws_exception()
      {
         var ds = new DataSet(new DataField<string>("s"));

         Assert.Throws<ArgumentException>(() => ds.Add(4));
      }

      [Fact]
      public void Validate_wrongcolnumber_throws_exception()
      {
         var ds = new DataSet(new DataField<string>("s"));

         Assert.Throws<ArgumentException>(() => ds.Add("1", 2));
      }

      [Fact]
      public void Can_add_repeatable_fields_of_incompatible_types()
      {
         var ds = new DataSet(
            new DataField<int>("id"),
            new DataField<IEnumerable<string>>("rep"));

         ds.Add(1, new[] { "one", "two" });
      }
   }
}
