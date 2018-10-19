using System;
using System.IO;
using System.Linq;
using SharpArrow.Data;
using Xunit;

namespace SharpArrow.Test
{
   public class DiskReadTest : TestBase
   {
      [Fact]
      public void Read_simple_three_cols_schema()
      {
         ArrowFile file = Arrow.OpenFromFile(GetDataFilePath("threecols.dat"));

         Schema schema = file.Schema;

         //validate schema

         Assert.Equal(3, schema.Fields.Length);

         Field f0 = schema.Fields[0];
         Field f1 = schema.Fields[1];
         Field f2 = schema.Fields[2];

         Assert.True(f0.IsNullable);
         Assert.True(f1.IsNullable);
         Assert.True(f2.IsNullable);

         Assert.Equal("f0", f0.Name);
         Assert.Equal("f1", f1.Name);
         Assert.Equal("f2", f2.Name);

         Assert.Equal(ArrowType.Int, f0.Type);
         Assert.Equal(ArrowType.Utf8, f1.Type);
         Assert.Equal(ArrowType.Bool, f2.Type);
      }

      [Fact]
      public void Read_data_in_thee_cols_file()
      {
         ArrowFile file = Arrow.OpenFromFile(GetDataFilePath("threecols.dat"));

         ArrowStream stream = file.GetStream();

         stream.TempTest();
      }
   }
}
