using System;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Parquet.Data.Rows;
using NetBox.Extensions;
using static Cpf.PoshConsole;

namespace Parquet.CLI.Commands
{
   class ConvertCommand : FileInputCommand
   {
      private readonly string _inputPath;
      private readonly string _output;
      private readonly string _style;
      private readonly bool _pretty;

      public ConvertCommand(string input, string output, string style, bool noColour) : base(input)
      {
         _inputPath = input;
         _output = output;
         _style = style;
         _pretty = noColour;
      }

      public void Execute()
      {
         string sourceExtension = Path.GetExtension(_inputPath);

         if(sourceExtension != ".parquet")
         {
            throw new ArgumentException($"Don't know how to read {sourceExtension}");
         }

         ConvertFromParquet();
      }

      private void ConvertFromParquet()
      {
         Telemetry.CommandExecuted("convert",
            "input", _inputPath,
            "output", _output,
            "style", _style,
            "pretty", _pretty);

         Table t = ReadTable();

         if(_pretty)
         {
            WriteLine("[", BracketColor);
         }

         int i = 0;
         foreach(Row row in t)
         {
            string json = row.ToString("j");

            if(!_pretty)
            {
               WriteLine(json);
            }
            else
            {
               WriteColorJson(json, ++i < t.Count);
            }
         }

         if (_pretty)
         {
            WriteLine("]", BracketColor);
         }
      }

      private const ConsoleColor BracketColor = ConsoleColor.DarkGray;
      private const ConsoleColor QuoteColor = ConsoleColor.Yellow;
      private const ConsoleColor PropertyNameColor = ConsoleColor.Green;
      private const ConsoleColor ValueColor = ConsoleColor.White;

      private void WriteColorJson(string json, bool hasMore)
      {
         JToken jt = JToken.Parse(json);

         Write(jt.ToString(Formatting.Indented));

         if(hasMore)
         {
            Write(",");
            WriteLine();
         }
      }
   }
}