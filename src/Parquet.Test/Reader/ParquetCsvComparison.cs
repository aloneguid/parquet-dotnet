using NetBox.FileFormats;
using Parquet.Data;
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Xunit;
using F = System.IO.File;
using Path = System.IO.Path;
using Type = System.Type;

namespace Parquet.Test.Reader
{
   public class ParquetCsvComparison : TestBase
   {
      protected void CompareFiles(string baseName, string encoding, params Type[] columnTypes)
      {
         DataSet parquet = ReadParquet($"{baseName}.{encoding}.parquet");
         DataSet csv = ReadCsv($"{baseName}.csv");
         Compare(parquet, csv, columnTypes);
      }

      private void Compare(DataSet parquet, DataSet csv, Type[] columnTypes)
      {
         //compar number of columns is the same
         Assert.Equal(parquet.FieldCount, csv.FieldCount);

         //compare column names
         foreach(string pq in parquet.Schema.FieldNames)
         {
            Assert.Contains(pq, csv.Schema.FieldNames);
         }

         //compare column values one by one
         Assert.True(columnTypes.Length == csv.FieldCount,
            $"incorrect type count, expected {csv.Count} but found {columnTypes.Length}");

         //compare individual rows
         for(int i = 0; i < parquet.RowCount; i++)
         {
            Row pr = parquet[i];
            Row cr = csv[i];

            //validate actual values
            Compare(parquet.Schema, pr, cr);
         }
      }

      private void Compare(Schema schema, Row pc, Row cc)
      {
         for(int i = 0; i < pc.Length; i++)
         {
            //todo: this comparison needs to be improved, probably doesn't handle nulls etc.

            Field se = schema.Fields[i];
            Type clrType = DataTypeFactory.Match(se).ClrType;

            object pv = pc[i];
            object cv = ChangeType(cc[i], clrType);

            if (pv == null)
            {
               bool isCsvNull =
                  cv == null ||
                  (cv is string s && s == string.Empty);

               Assert.True(isCsvNull,
                  $"expected null value in column {se.Name}, value #{i}");
            }
            else
            {
               if (clrType == typeof(string))
               {
                  Assert.True(((string)pv).Trim() == ((string)cv).Trim(),
                     $"expected {cv} but was {pv} in column {se.Name}, value #{i}");
               }
               else
               {
                  Assert.True(pv.Equals(cv),
                     $"expected {cv} but was {pv} in column {se.Name}, value #{i}");
               }
            }
         }
      }

      private object ChangeType(object v, Type t)
      {
         if (v == null) return null;
         if (v.GetType() == t) return v;
         if (v is string s && string.IsNullOrEmpty(s)) return null;

         if(t == typeof(DateTimeOffset))
         {
            string so = (string)v;
            return new DateTimeOffset(DateTime.Parse(so));
         }

         return Convert.ChangeType(v, t);
      }

      private DataSet ReadParquet(string name)
      {
         using (Stream s = OpenTestFile(name))
         {
            return ParquetReader.Read(s, new ParquetOptions { TreatByteArrayAsString = true });
         }
      }

      private DataSet ReadCsv(string name)
      {
         DataSet result;

         using (Stream fs = OpenTestFile(name))
         {
            var reader = new CsvReader(fs, Encoding.UTF8);

            //header
            string[] columnNames = reader.ReadNextRow();
            result = new DataSet(new Schema(columnNames.Select(n => new DataField(n, DataType.String))));

            //values
            string[] values;
            while((values = reader.ReadNextRow()) != null)
            {
               var row = new Row(values);
               result.Add(row);
            }
         }
         return result;
      }
   }
}
