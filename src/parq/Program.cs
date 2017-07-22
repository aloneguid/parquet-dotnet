using System;
using Parquet;
using parq.Display;
using parq.Display.Views;
using Parquet.Data;

namespace parq
{

   class Program
   {
      static void Main(string[] args)
      {
         if (AppSettings.Instance.ShowVersion)
         {
            Console.WriteLine(GetVersionNumber(typeof(Program).AssemblyQualifiedName));
            return;
         }

         if (string.IsNullOrEmpty(AppSettings.Instance.InputFilePath))
         {
            WriteHelp();
         }
         else
         {
            var path = System.IO.Path.Combine(AppContext.BaseDirectory, AppSettings.Instance.InputFilePath);
            Verbose("Input file chosen as {0}", path);

            if (!System.IO.File.Exists(path))
            {
               Console.Error.WriteLine("The path {0} does not exist", path);
               return;
            }
            else
            {
               long fileLen = 0;
               var dataSet = ReadFromParquetFile(path, out fileLen);

               // After reading the column types give a printed list of the layout of the columns 
               var display = new DisplayController();
               var viewModel = display.Get(dataSet);

               if (string.Compare(AppSettings.Instance.Mode, "interactive", true) == 0)
               {
                  new InteractiveConsoleView().Draw(viewModel);
               }
               else if (string.Compare(AppSettings.Instance.Mode, "full", true) == 0)
               {
                  new FullConsoleView().Draw(viewModel);
               }
               else if (string.Compare(AppSettings.Instance.Mode, "schema", true) == 0)
               {
                  new SchemaView().Draw(viewModel);
               }

            }
         }
      }

      private static string GetVersionNumber(string assemblyQualifiedName)
      {
         var fromVersion = (assemblyQualifiedName.Substring(assemblyQualifiedName.IndexOf("Version=") + 8));
         return fromVersion.Substring(0, fromVersion.IndexOf(','));
      }

      private static void Verbose(string format, params string[] path)
      {
         Console.WriteLine(format, path);
      }

      public static DataSet ReadFromParquetFile(string path, out long fileLen)
      {
         var fileInfo = new System.IO.FileInfo(path);
         fileLen = fileInfo.Length;
         return ParquetReader.ReadFile(path);
      }

      private static void WriteHelp()
      {
         Console.WriteLine("dotnet parq.dll\t-\tParquet File Inspector for .net\n");
         Console.WriteLine("Usage\t\t-\tparq.exe operation InputFilePath=[relativeStringPath] DisplayMinWidth=[10]");
         Console.WriteLine("\t\t\tOperation one of: interactive (default), full, schema");
      }
   }
}