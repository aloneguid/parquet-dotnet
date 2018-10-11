using System;
using System.Collections.Generic;
using System.Text;
using LogMagic;

namespace Parquet.CLI
{
   /// <summary>
   /// Telemetry constants
   /// </summary>
   static class Telemetry
   {
      private static readonly ILog log = L.G(typeof(Telemetry));

      public const string ExecuteCommand = "Exec";

      public static void CliInvoked(string[] args)
      {
         log.Event("CliInvoked",
            "OS", Environment.OSVersion.VersionString,
            "RawArgs", string.Join(';', args));
      }

      public static void CommandExecuted(string name, params object[] properties)
      {
         var psl = new List<object>(properties);
         psl.Add("name");
         psl.Add(name);

         log.Event(ExecuteCommand, psl.ToArray());
      }
   }
}
