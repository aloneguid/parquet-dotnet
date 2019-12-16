using System;
using NetBox.Terminal.App;
using Parquet.CLI.Commands;
using Serilog;
using static NetBox.Terminal.PoshConsole;

namespace Parquet.CLI
{
   /// <summary>
   /// This is a parquet.net dotnet CLI tool that can be installed globally. An ultimate replacement for "parq".
   /// It is very much in progress and in design.
   /// 
   /// It's using CPF library for handling console commands which is very fresh and unstable. Why? Because no one
   /// ever 
   /// </summary>
   class Program
   {
      static int Main(string[] args)
      {
         var app = new Application("Parquet CLI (https://github.com/aloneguid/parquet-dotnet) by Ivan Gavryliuk.");
         ConfigureTelemetry(app, args);

         LinePrimitive<bool> verboseOption = app.SharedOption<bool>("-d|--debug", Help.App_Verbose);

         app.OnBeforeExecuteCommand(cmd =>
         {
            PoshWrite("{p}{a}{r}{q} v", ConsoleColor.Yellow, ConsoleColor.Red, ConsoleColor.Green, ConsoleColor.Blue);

            string[] pts = app.Version.Split('.');
            for (int i = 0; i < pts.Length; i++)
            {
               if (i > 0)
                  Write(".", ConsoleColor.DarkGray);

               Write(pts[i], ConsoleColor.Green);
            }
            WriteLine();
            WriteLine();
         });

         app.OnError((cmd, err) =>
         {
            Log.Error(err, "error in command {command}", cmd.Name);
            return true;
         });

         app.Command("schema", cmd =>
         {
            cmd.Description = Help.Command_Schema_Description;

            LinePrimitive<string> path = cmd.Argument<string>("path", Help.Argument_Path).Required().FileExists();

            cmd.OnExecute(() =>
            {
               new SchemaCommand(path.Value).Execute();
            });
         });

         app.Command("meta", cmd =>
         {
            cmd.Description = Help.Command_Meta_Description;

            LinePrimitive<string> path = cmd.Argument<string>("path", Help.Argument_Path).Required().FileExists();

            cmd.OnExecute(() =>
            {
               new MetaCommand(path).Execute();
            });
         });

         app.Command("convert", cmd =>
         {
            cmd.Description = Help.Command_Convert_Description;

            LinePrimitive<string> input = cmd.Argument<string>("input", Help.Command_Convert_Input).Required().FileExists();
            //LinePrimitive<string> output = cmd.Argument<string>("output", Help.Command_Convert_Output);
            //LinePrimitive<string> style = cmd.Option<string>("-s|--style", Help.Command_Convert_Style);
            LinePrimitive<bool> pretty = cmd.Option<bool>("-p|--pretty", Help.Command_Convert_Pretty);
            LinePrimitive<int> maxRows = cmd.Option<int>("-m|--max-rows", Help.Command_Convert_MaxRows, 10);

            cmd.OnExecute(() =>
            {
               new ConvertCommand(input, null, null, pretty).Execute(maxRows);
            });
         });

         int exitCode = app.Execute();

         Log.CloseAndFlush();

         return exitCode;
      }

      private static void ConfigureTelemetry(Application app, string[] args)
      {
         //let's see if we get complains about performance here, should be OK

         /*
          * If you're wondering whether hardcoding the key is good or not - it doesn't really matter. The worst you can _attempt_ to do
          * is send us a lot of fake telemetry but we can live with it. Even if we would hide the key you could still do the same thing
          * by launching application instead of calling appinsights REST API.
          */

         /*var configuration = new TelemetryConfiguration
         {
            InstrumentationKey = "0a310ae1-0f93-43fc-bfa1-62e92fc869b9",
            TelemetryChannel = new PersistenceChannel()
         };*/

         Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .Enrich.WithProperty("Version", app.Version)
            .WriteTo.ApplicationInsights("aaf3c0f7-dc49-466c-848d-49ccfcdf86fe", TelemetryConverter.Events)
            .WriteTo.Trace()
            .CreateLogger();

         Telemetry.CliInvoked(args);
      }
   }
}