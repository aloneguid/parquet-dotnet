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

      public void Execute(int maxRows, string format)
      {
         string fmt = format switch
         {
            "json" => "j",
            "csv" => "c",
            _ => throw new ArgumentException($"unknown format '{format}'")
         };

         ConvertFromParquet(maxRows, fmt);
      }

      private void ConvertFromParquet(int maxRows, string fmt)
      {
         Telemetry.CommandExecuted("convert",
            "input", _inputPath,
            "maxRows", maxRows,
            "fmt", fmt);

         Table t = ReadTable(maxRows);

         int i = 0;
         foreach(Row row in t)
         {
            if (i >= maxRows)
               break;

            string json = row.ToString(fmt, i);

            WriteLine(json);
            i++;
         }
      }
   }
}