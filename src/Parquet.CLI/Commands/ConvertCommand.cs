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
            "output", _output,
            "style", _style,
            "pretty", _pretty,
            "maxRows", maxRows);

         Table t = ReadTable(maxRows);

         if(_pretty)
         {
            Write("[", BracketColor);
         }

         int i = 0;
         foreach(Row row in t)
         {
            if (i >= maxRows)
               break;

            string json = row.ToString("j");

            if(!_pretty)
            {
               WriteLine(json);
               i++;
            }
            else
            {
               WriteColorJson(json, ++i < t.Count && i < maxRows);
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