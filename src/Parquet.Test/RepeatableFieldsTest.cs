using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class RepeatableFieldsTest
   {
      [Fact]
      public void Simple_repeated_field_write_read()
      {
         var ds = new DataSet(
            new DataField<int>("id"),
            new DataField<IEnumerable<string>>("items"));

         ds.Add(1, new[] { "one", "two" });

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal(1, ds1[0][0]);
         Assert.Equal(new[] { "one", "two" }, ds1[0][1]);
      }

      [Fact]
      public void Repeatable_field_writes_reads()
      {
         var ds = new DataSet(new DataField<int>("id"), new DataField<IEnumerable<string>>("repeats"));
         ds.Add(1, new[] { "one", "two", "three" });

         Assert.Equal("{1;[one;two;three]}", DataSetGenerator.WriteRead(ds)[0].ToString());
      }

      [Fact]
      public void Repeatable_field_with_no_values_writes_reads()
      {
         var ds = new DataSet(new DataField<int>("id"), new DataField<IEnumerable<string>>("repeats"));
         ds.Add(1, new string[0]);

         DataSet ds1 = ds.WriteRead();

         Assert.Equal("{1;[]}", ds1[0].ToString());
      }
   }
}
