using NetBox;
using NetBox.FileFormats;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
   public class ParquetCsvComparison
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
         Assert.Equal(parquet.ColumnCount, csv.ColumnCount);

         //compare column names
         foreach(string pq in parquet.Schema.ColumnNames)
         {
            Assert.True(csv.Schema.ColumnNames.Contains(pq));
         }

         //compare column values one by one
         Assert.True(columnTypes.Length == csv.ColumnCount,
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

            SchemaElement se = schema.Elements[i];

            object pv = pc[i];
            object cv = ChangeType(cc[i], se.ElementType);

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
               if (se.ElementType == typeof(string))
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
         string path = GetDataFilePath(name);
         return ParquetReader.ReadFile(path, new ParquetOptions { TreatByteArrayAsString = true });
      }

      private DataSet ReadCsv(string name)
      {
         DataSet result;

         using (Stream fs = F.OpenRead(GetDataFilePath(name)))
         {
            var reader = new CsvReader(fs, Encoding.UTF8);

            //header
            string[] columnNames = reader.ReadNextRow();
            result = new DataSet(new Schema(columnNames.Select(n => new SchemaElement(n, typeof(string)))));

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

      private string GetDataFilePath(string name)
      {
         string thisPath = Assembly.Load(new AssemblyName("Parquet.Test")).Location;
         return Path.Combine(Path.GetDirectoryName(thisPath), "data", name);
      }
   }
}
