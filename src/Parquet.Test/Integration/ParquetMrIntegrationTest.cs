using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Parquet.Data;
using Parquet.Data.Rows;
using Xunit;
using F = System.IO.File;

namespace Parquet.Test.Integration
{
   /// <summary>
   /// This class does some fairly basic integration tests by compring results with parquet-mr using parquet-tools jar package.
   /// You must have java available in PATH.
   /// </summary>
   public class ParquetMrIntegrationTest : TestBase
   {
      private readonly string _toolsPath;
      private readonly string _toolsJarPath;

      public ParquetMrIntegrationTest()
      {
         _toolsPath = Path.GetFullPath("..\\..\\..\\..\\..\\tools");
         _toolsJarPath = Path.Combine(_toolsPath, "parquet-tools-1.9.0.jar");
      }

      private void CompareWithMr(Table t)
      {
         string testFileName = Path.GetFullPath("temp.parquet");

         if (F.Exists(testFileName))
            F.Delete(testFileName);

         //produce file
         using (Stream s = F.OpenWrite(testFileName))
         {
            using (var writer = new ParquetWriter(t.Schema, s))
            {
               writer.Write(t);
            }
         }

         //read back
         Table t2 = ParquetReader.ReadTableFromFile(testFileName);

         //check we don't have a bug internally before launching MR
         Assert.Equal(t.ToString("j"), t2.ToString("j"), ignoreLineEndingDifferences: true);

         string mrJson = ExecAndGetOutput("java.exe", $"-jar {_toolsJarPath} cat -j {testFileName}");
         Assert.Equal(t.ToString("j"), mrJson);
      }

      private static string ExecAndGetOutput(string fileName, string arguments)
      {
         var psi = new ProcessStartInfo
         {
            FileName = fileName,
            Arguments = arguments,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            CreateNoWindow = true
         };

         var proc = new Process { StartInfo = psi };

         if (!proc.Start())
            return null;

         var res = new StringBuilder();

         while (!proc.StandardOutput.EndOfStream)
         {
            string line = proc.StandardOutput.ReadLine();
            res.AppendLine(line);
         }

         proc.WaitForExit();

         return res.ToString().Trim();
      }

      [Fact]
      public void Flat_simple_table()
      {
         var table = new Table(new DataField<int>("id"), new DataField<string>("city"));

         //generate fake data
         for (int i = 0; i < 1000; i++)
         {
            table.Add(new Row(i, "record#" + i));
         }

         CompareWithMr(table);
      }

      [Fact]
      public void Array_simple_integers()
      {
         var table = new Table(
            new DataField<int>("id"),
            new DataField<string[]>("categories")     //array field
         );

         table.Add(1, new[] { "1", "2", "3" });
         table.Add(3, new[] { "3", "3", "3" });

         CompareWithMr(table);
      }

   }
}