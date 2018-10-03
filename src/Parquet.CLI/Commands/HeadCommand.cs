using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data.Rows;
using static Cpf.PoshConsole;

namespace Parquet.CLI.Commands
{
   class HeadCommand : FileInputCommand
   {
      private readonly int _max;

      public HeadCommand(string path, int max) : base(path)
      {
         _max = max;

         if (_max > 100)
            _max = 100;

         if (_max < 1)
            _max = 1;
      }

      public void Execute(string format)
      {
         Write("displaying first ");
         Write(_max.ToString(), T.ActiveTextColor);
         Write(" records...");
         WriteLine();

         Table table = ReadTable();

         WriteLine(table.ToString("mjn"));

         WriteLine("work in progress!!!!", T.ErrorTextColor);
      }
   }
}
