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

      [Fact]
      public void Can_merge_two_datasets()
      {
         var ds1 = new DataSet(new DataField<int>("id"));
         ds1.Add(1);
         ds1.Add(2);

         var ds2 = new DataSet(new DataField<int>("id"));
         ds2.Add(3);
         ds2.Add(4);

         ds1.Merge(ds2);

         Assert.Equal(4, ds1.Count);

         IReadOnlyCollection<int> column = ds1.GetColumn<int>((DataField)ds1.Schema[0]);
         Assert.Equal(4, column.Count);
         Assert.Equal(new[] { 1, 2, 3, 4 }, column);
      }

      [Fact]
      public void Cannot_merge_datasets_with_incompatible_schemas()
      {
         var ds1 = new DataSet(new DataField<int>("id"));
         var ds2 = new DataSet(new DataField<int?>("id"));

         Assert.Throws<ArgumentException>(() => ds1.Merge(ds2));
      }
   }
}
