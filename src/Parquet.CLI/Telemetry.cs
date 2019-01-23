using System;
using Serilog;

namespace Parquet.CLI
{
   /// <summary>
   /// Telemetry constants
   /// </summary>
   static class Telemetry
   {
      public const string ExecuteCommand = "Exec";

      public static void CliInvoked(string[] args)
      {
         Log.Information("CliInvoked (os: {os}, args: {args}",
            Environment.OSVersion.VersionString,
            string.Join(';', args));
      }

      public static void CommandExecuted(string name, params object[] properties)
      {
         Log.Information("command {command} executed", name);
      }
   }
}
