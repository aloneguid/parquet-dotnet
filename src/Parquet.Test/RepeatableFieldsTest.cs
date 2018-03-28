using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;
using Parquet.File;
using Xunit;

namespace Parquet.Test
{
   public class RepeatableFieldsTest : TestBase
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
      public void Simple_repeated_field_write_read_v3()
      {
         // arrange 
         var field = new DataField<IEnumerable<int>>("items");
         var column = new DataColumn(field);
         column.IncrementLevel();
         column.Add(1);
         column.Add(2);
         column.DecrementLevel();
         column.IncrementLevel();
         column.Add(3);
         column.Add(4);
         column.DecrementLevel();

         // act
         DataColumn rc = WriteReadSingleColumn(field, 2, column, true);

         // assert
         Assert.Equal(new int[] { 1, 2, 3, 4 }, rc.DefinedData);
         Assert.Equal(new int[] { 1, 1, 1, 1 }, rc.DefinitionLevels);
         Assert.Equal(new int[] { 0, 1, 0, 1 }, rc.RepetitionLevels);


         
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
