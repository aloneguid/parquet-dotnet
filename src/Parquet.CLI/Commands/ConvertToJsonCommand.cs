using System;

namespace Parquet.CLI.Commands
{
   class ConvertToJsonCommand : FileInputCommand
   {
      private const ConsoleColor BracketColor = ConsoleColor.Yellow;
      private const ConsoleColor NameColor = ConsoleColor.DarkGray;

      public ConvertToJsonCommand(string path) : base(path)
      {
      }

      public void Execute()
      {
       
      }
   }
}
