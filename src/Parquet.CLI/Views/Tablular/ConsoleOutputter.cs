using System;
using Parquet.CLI.Models;

namespace Parquet.CLI.Views.Tablular
{
   internal class ConsoleOutputter : IConsoleOutputter
   {
      public ConsoleColor BackgroundColor { get; set; }

      public void ResetColor()
      {
         Console.ResetColor();
      }

      public void SetForegroundColor(ConsoleColor foreColor)
      {
         Console.ForegroundColor = foreColor;
      }

      public void Write(string s)
      {
         Console.Write(s);
      }
   }
}