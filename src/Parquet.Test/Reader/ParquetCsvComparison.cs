using NetBox;
using NetBox.FileFormats;
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
         ParquetDataSet parquet = ReadParquet($"{baseName}.{encoding}.parquet");
         var csv = ReadCsv($"{baseName}.csv");
         Compare(parquet, csv, columnTypes);
      }

      private void Compare(ParquetDataSet parquet, Dictionary<string, List<string>> csv, Type[] columnTypes)
      {
         //compar number of columns is the same
         Assert.Equal(parquet.Columns.Count, csv.Count);

         //compare column names
         foreach(ParquetColumn pq in parquet.Columns)
         {
            Assert.True(csv.ContainsKey(pq.Name));
         }

         //compare column values one by one
         Assert.True(columnTypes.Length == csv.Count,
            $"incorrect type count, expected {csv.Count} but found {columnTypes.Length}");

         //compare individual columns
         int i = 0;
         foreach(ParquetColumn pc in parquet.Columns)
         {
            List<string> cc = csv[pc.Name];
            Type expectedColumnType = columnTypes[i];

            //validate column type
            Assert.True(expectedColumnType == pc.SystemType, $"expected {expectedColumnType} for column {pc.Name}#{i} but found {pc.SystemType}");

            //validate number of values
            Assert.True(cc.Count == pc.Values.Count, $"expected {cc.Count} values in column {pc.Name} but found {pc.Values.Count}");

            //validate actual values
            Compare(pc, cc);

            i += 1;
         }
      }

      private void Compare(ParquetColumn pc, List<string> cc)
      {
         for(int i = 0; i < pc.Values.Count; i++)
         {
            //todo: this comparison needs to be improved, probably doesn't handle nulls etc.

            object pv = pc.Values[i];
            object cv = ChangeType(cc[i], pc.SystemType);

            if (pv == null)
            {
               bool isCsvNull =
                  cv == null ||
                  (cv is string s && s == string.Empty);

               Assert.True(isCsvNull,
                  $"expected null value in column {pc.Name}, value #{i}");
            }
            else
            {
               if (pc.SystemType == typeof(string))
               {
                  Assert.True(((string)pv).Trim() == ((string)cv).Trim(),
                     $"expected {cv} but was {pv} in column {pc.Name}, value #{i}");
               }
               else
               {
                  Assert.True(pv.Equals(cv),
                     $"expected {cv} but was {pv} in column {pc.Name}, value #{i}");
               }
            }
         }
      }

      private object ChangeType(string v, Type t)
      {
         Type gt = null;

         try
         {
            gt = t.GetGenericTypeDefinition();
         }
         catch(InvalidOperationException)
         {
            //when type is not generic
         }

         if (gt != null && gt == typeof(Nullable<>))
         {
            Type tt = t.GenericTypeArguments[0];

            try
            {
               if (tt == typeof(DateTimeOffset)) return v == string.Empty ? null : new DateTimeOffset?(new DateTimeOffset(DateTime.Parse(v)));

               return Convert.ChangeType(v, tt);
            }
            catch(FormatException)
            {
               var dv = new DynamicValue(v);
               return dv.GetValue(tt);
            }
         }

         return Convert.ChangeType(v, t);
      }

      private ParquetDataSet ReadParquet(string name)
      {
         ParquetDataSet parquet;
         using (Stream fs = F.OpenRead(GetDataFilePath(name)))
         {
            using (ParquetReader reader = new ParquetReader(fs))
            {
               reader.Options.TreatByteArrayAsString = true;

               parquet = reader.Read();
            }
         }
         return parquet;
      }

      private Dictionary<string, List<string>> ReadCsv(string name)
      {
         var result = new Dictionary<string, List<string>>();
         using (Stream fs = F.OpenRead(GetDataFilePath(name)))
         {
            var reader = new CsvReader(fs, Encoding.UTF8);

            //header
            string[] columnNames = reader.ReadNextRow();
            foreach (string columnName in columnNames) result[columnName] = new List<string>();

            //values
            string[] values;
            while((values = reader.ReadNextRow()) != null)
            {
               for(int i = 0; i < values.Length; i++)
               {
                  result[columnNames[i]].Add(values[i]);
               }
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
