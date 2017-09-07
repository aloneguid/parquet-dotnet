using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;

namespace Parquet.Test
{
   public class MergeTest
   {
      [Fact]
      public void Merge_DataSet_NoColsSame_Success_NumCols()
      {
         var ds1 = new DataSet(new SchemaElement<int>("id"))
         {
            1
         };
         var ds2 = new DataSet(new SchemaElement<DateTime>("iddtt2"))
         {
            DateTime.UtcNow
         };

         DataSet ds3 = ds1.Merge(ds2);
         Assert.Equal(2, ds3.ColumnCount);
      }

      [Fact]
      public void Merge_DataSet_NoColsSame_Success_NumRows()
      {
         var ds1 = new DataSet(new SchemaElement<int>("id"))
         {
            1
         };
         var ds2 = new DataSet(new SchemaElement<DateTime>("iddtt2"))
         {
            DateTime.UtcNow
         };

         DataSet ds3 = ds1.Merge(ds2);
         Assert.Equal(2, ds3.RowCount);
      }

      [Fact]
      public void Merge_DataSet_NoColsSame_Success_TypesCorrect()
      {
         var ds1 = new DataSet(new SchemaElement<int>("id"))
         {
            1
         };
         var ds2 = new DataSet(new SchemaElement<DateTime>("iddtt2"))
         {
            new DateTime(2003, 10, 1)
         };

         DataSet ds3 = ds1.Merge(ds2);
         Assert.Equal(1, ds3[0][0]);
         Assert.Equal(null, ds3[0][1]);
         Assert.Equal(null, ds3[1][0]);
         Assert.Equal(new DateTime(2003, 10, 1), ds3[1][1]);
      }

      [Fact]
      public void Merge_DataSet_OneColSame_Success()
      {
         var ds1 = new DataSet(new SchemaElement<int>("id"))
         {
            1
         };
         var ds2 = new DataSet(new SchemaElement<int>("id"))
         {
            2
         };

         DataSet ds3 = ds1.Merge(ds2);
         Assert.Equal(2, ds3.RowCount);
         Assert.Equal(1, ds3.ColumnCount);
         Assert.Equal(1, ds3[0][0]);
         Assert.Equal(2, ds3[1][0]);
      }

      [Fact]
      public void Merge_DataSet_NameColTypeDifferent_Exception()
      {
         var ds1 = new DataSet(new SchemaElement<int>("id"))
         {
            1
         };
         var ds2 = new DataSet(new SchemaElement<DateTime>("id"))
         {
            DateTime.UtcNow
         };

         Assert.Throws<ParquetException>(() => ds1.Merge(ds2));
      }
   }
}
