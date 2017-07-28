using System;
using Parquet;
using parq.Display;
using parq.Display.Views;
using Parquet.Data;
using Config.Net;

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
               // After reading the column types give a printed list of the layout of the columns 
               var display = new DisplayController();


               if (string.Compare(AppSettings.Instance.Mode, "interactive", true) == 0)
               {
                  long fileLen = 0;
                  var view = new InteractiveConsoleView();
                  view.FoldRequested += (object sender, Display.Models.ConsoleFold cf) =>
                  {
                     var nextDataSet = ReadFromParquetFileOffset(path, cf.IndexStart, cf.IndexEnd - cf.IndexStart, out fileLen);
                     var updatedViewModel = display.Get(nextDataSet);
                     view.Update(updatedViewModel);
                  };

                  var dataSet = ReadFromParquetFileOffset(path, 0, view.GetRowCount(), out fileLen);
                  var viewModel = display.Get(dataSet);
                  view.Draw(viewModel);
               }
               else if (string.Compare(AppSettings.Instance.Mode, "head", true) == 0)
               {
                  if (AppSettings.Instance.Head != null)
                  {
                     long fileLen;
                     var view = new FullConsoleView();
                     var dataSet = ReadFromParquetFileOffset(path, 0, AppSettings.Instance.Head, out fileLen);
                     var viewModel = display.Get(dataSet);
                     view.Draw(viewModel);
                  }
                  else
                  {
                     WriteHelp();
                  }
               }
               else if (string.Compare(AppSettings.Instance.Mode, "tail", true) == 0)
               {
                  if (AppSettings.Instance.Tail != null)
                  {
                     long fileLen;
                     var view = new FullConsoleView();
                     var dataSet = ReadFromParquetFileTail(path, AppSettings.Instance.Tail, out fileLen);
                     var viewModel = display.Get(dataSet);
                     view.Draw(viewModel);
                  }
                  else
                  {
                     WriteHelp();
                  }
               }
               else
               {
                  long fileLen = 0;
                  var dataSet = ReadFromParquetFile(path, out fileLen);
                  var viewModel = display.Get(dataSet);
                  if (string.Compare(AppSettings.Instance.Mode, "full", true) == 0)
                  {
                     new FullConsoleView().Draw(viewModel);
                  }
                  else if (string.Compare(AppSettings.Instance.Mode, "schema", true) == 0)
                  {
                     new SchemaView().Draw(viewModel);
                  }
                  else if (string.Compare(AppSettings.Instance.Mode, "rowcount", true) == 0)
                  {
                     new RowCountView().Draw(viewModel);
                  }
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

      public static DataSet ReadFromParquetFileOffset(string path, long skip, long take, out long fileLen)
      {
         var fileInfo = new System.IO.FileInfo(path);
         fileLen = fileInfo.Length;
         return ParquetReader.ReadFile(path, null, new ReaderOptions() { Count = (int)take, Offset = (int)skip });
      }

      public static DataSet ReadFromParquetFile(string path, out long fileLen)
      {
         var fileInfo = new System.IO.FileInfo(path);
         fileLen = fileInfo.Length;
         return ParquetReader.ReadFile(path);
      }

      private static DataSet ReadFromParquetFileTail(string path, Option<int> tail, out long fileLen)
      {
         var peekedFileManifest = ReadFromParquetFileOffset(path, 0, 1, out fileLen);
         return ReadFromParquetFileOffset(path, peekedFileManifest.TotalRowCount - tail.Value, tail.Value, out fileLen);
      }

      private static void WriteHelp()
      {
         Console.WriteLine("dotnet parq.dll\t-\tParquet File Inspector for .net\n");
         Console.WriteLine("Usage\t\t-\tparq.exe Mode=operation InputFilePath=[relativeStringPath] DisplayMinWidth=[10]");
         Console.WriteLine("\t\t\tOperation one of: interactive (default), full, schema, rowcount, head, tail");
      }
   }
}