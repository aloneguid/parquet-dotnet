using System;
using LogMagic;
using Parquet;
using parq.Display;
using parq.Display.Views;
using Parquet.Data;

namespace parq
{

   class Program
   {
      static ILog _log = L.G<Program>();

      static void Main(string[] args)
      {
         L.Config.WriteTo.PoshConsole();

         if (string.IsNullOrEmpty(AppSettings.Instance.InputFilePath))
         {
            WriteHelp();
         }
         else
         {
            var path = System.IO.Path.Combine(AppContext.BaseDirectory, AppSettings.Instance.InputFilePath);
            _log.D("Input file chosen as {0}", path);

            if (!System.IO.File.Exists(path))
            {
               _log.E("The path {0} does not exist", path);
               return;
            }
            else
            {
               long fileLen = 0;
               var dataSet = ReadFromParquetFile(path, out fileLen);
               _log.I("The file has a length of {0}", fileLen);


               foreach (SchemaElement column in dataSet.Schema.Elements)
               {
                  _log.I("{0} - {1}", column.Name, column.ElementType);
               }


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

               }

            }
         }
      }

      public static DataSet ReadFromParquetFile(string path, out long fileLen)
      {
         var fileInfo = new System.IO.FileInfo(path);
         fileLen = fileInfo.Length;
         return ParquetReader.ReadFile(path);
      }

      private static void WriteHelp()
      {
         _log.I("dotnet parq.dll\t-\tParquet File Inspector for .net");
         _log.I("Usage\t-\tparq.exe InputFilePath=[relativeStringPath] DisplayMinWidth=[10]");
      }
   }
}