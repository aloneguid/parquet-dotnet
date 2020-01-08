using System;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Parquet.Data.Rows;
using NetBox.Extensions;
using static NetBox.Terminal.PoshConsole;

namespace Parquet.CLI.Commands
{
   class ConvertCommand : FileInputCommand
   {
      private readonly string _inputPath;

      public ConvertCommand(string input) : base(input)
      {
         _inputPath = input;
      }

      public void Execute(int maxRows)
      {
         string sourceExtension = Path.GetExtension(_inputPath);

         if(sourceExtension != ".parquet")
         {
            throw new ArgumentException($"Don't know how to read {sourceExtension}");
         }

         ConvertFromParquet(maxRows);
      }

      private void ConvertFromParquet(int maxRows)
      {
         Telemetry.CommandExecuted("convert",
            "input", _inputPath,
            "maxRows", maxRows);

         Table t = ReadTable(maxRows);

         int i = 0;
         foreach(Row row in t)
         {
            if (i >= maxRows)
               break;

            string json = row.ToString("j");

            WriteLine(json);
            i++;
         }
      }
   }
}