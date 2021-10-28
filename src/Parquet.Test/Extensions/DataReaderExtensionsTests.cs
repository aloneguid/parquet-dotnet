using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using Parquet.Data;
using Parquet.Extensions;
using Sylvan.Data.Csv;
using Xunit;

namespace Parquet.Test.Extensions
{
   public class DataReaderExtensionsTests
   {
      class CsvSchema : ICsvSchemaProvider
      {
         SchemaColumn[] columns;
         public CsvSchema(params Type[] types)
         {
            this.columns = types.Select(t => new SchemaColumn(t)).ToArray();
         }

         public DbColumn GetColumn(string name, int ordinal)
         {
            return columns[ordinal];
         }

         class SchemaColumn : DbColumn
         {
            public SchemaColumn(Type type)
            {
               this.DataType = type;
            }
         }
      }

      [Fact]
      public void WriteParquetTest1()
      {
         Type[] columnTypes = new Type[]
         {
            typeof(int),
            typeof(bool),
            typeof(byte),
            typeof(short),
            typeof(int),
            typeof(long),
            typeof(float),
            typeof(double),
            typeof(DateTime),
            typeof(string),
            // two date time columns, should they be expected to be treated differently ??
            typeof(DateTime) 
         };

         var schema = new CsvSchema(columnTypes);
         var opts = new CsvDataReaderOptions { Schema = schema };
         var dataReader = CsvDataReader.Create("data/types/alltypes.csv", opts);
         var stream = new MemoryStream();
         dataReader.WriteParquet(stream);

         // assert that we can read back the expected data
         stream.Seek(0, SeekOrigin.Begin);

         var reader = new ParquetReader(stream);
         DataField[] fields = reader.Schema.GetDataFields();
         Assert.Equal(11, fields.Length);
         Assert.Equal(1, reader.RowGroupCount);
         using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
         Assert.Equal(8, rgr.RowCount);

         for (int i = 0; i < fields.Length; i++)
         {
            DataColumn columnData = rgr.ReadColumn(fields[i]);
            Type type = columnTypes[i];
            if (type == typeof(DateTime))
            {
               // I don't like this.
               Assert.Equal(typeof(DateTimeOffset), columnData.Field.ClrType);
            }
            else
            {
               Assert.Equal(type, columnData.Field.ClrType);
            }
         }
      }
   }
}
