using System;
using LogMagic;
using Parquet;
using parq.Display;

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

                
               foreach (var column in dataSet.Columns)
               {
                  _log.I("{0} - {1}", column.Name, column.ParquetRawType);
               }


               // After reading the column types give a printed list of the layout of the columns 
               var display = new DisplayController();
               display.Draw(dataSet);

               Console.ReadLine();

            }
          }
       }

      public static ParquetDataSet ReadFromParquetFile(string path, out long fileLen)
       {
          var fileInfo = new System.IO.FileInfo(path);
          fileLen = fileInfo.Length;
          ParquetDataSet dataSet = null;
          using (var inStream = fileInfo.Open(System.IO.FileMode.Open))
          {
             using (var reader = new Parquet.ParquetReader(inStream))
             {
                dataSet = reader.Read();
             }
          }
          return dataSet;
       }

       private static void WriteHelp()
      { 
         _log.I("dotnet parq.dll\t-\tParquet File Inspector for .net");
         _log.I("Usage\t-\tparq.exe InputFilePath=[relativeStringPath] DisplayMinWidth=[10]");
      }
   }
}