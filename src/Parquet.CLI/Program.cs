using System;
using Cpf;
using Cpf.App;
using LogMagic;
using LogMagic.Enrichers;
using Parquet.CLI.Commands;
using Parquet.CLI.Models;
using static Cpf.PoshConsole;

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
      private static readonly ILog log = L.G(typeof(Program));

      static int Main(string[] args)
      {
         var app = new Application("Parquet CLI (https://github.com/elastacloud/parquet-dotnet)");
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
            log.Trace("error in command {command}", cmd.Name, err);
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

         app.Command("view-all", cmd =>
         {
            cmd.Description = Help.Command_ViewAll_Description;
            LinePrimitive<string> path = cmd.Argument<string>("path", Help.Argument_Path).Required().FileExists();
            LinePrimitive<bool> expandCells = cmd.Option<bool>("-e|--expand", Help.Command_ViewAll_Expand, false);
            LinePrimitive<int> displayMinWidth = cmd.Option<int>("-m|--min", Help.Command_ViewAll_Min, 5);
            LinePrimitive<bool> displayNulls = cmd.Option<bool>("-n|--nulls", Help.Command_ViewAll_Nulls, false);

            cmd.OnExecute(() =>
            {
               ViewSettings settings = new ViewSettings
               {
                  displayMinWidth = displayMinWidth,
                  displayNulls = displayNulls,
                  displayTypes = false,
                  expandCells = expandCells,
                  truncationIdentifier = string.Empty
               };

               new DisplayFullCommand<Views.FullConsoleView>(path).Execute(settings);
            });
         });

         app.Command("view", cmd =>
         {
            cmd.Description = Help.Command_View_Description;
            LinePrimitive<string> path = cmd.Argument<string>("path", Help.Argument_Path).Required();
            LinePrimitive<bool> expandCells = cmd.Option<bool>("-e|--expand", Help.Command_ViewAll_Expand, false);
            LinePrimitive<int> displayMinWidth = cmd.Option<int>("-m|--min", Help.Command_ViewAll_Min, 5);
            LinePrimitive<bool> displayNulls = cmd.Option<bool>("-n|--nulls", Help.Command_ViewAll_Nulls, true);
            LinePrimitive<bool> displayTypes = cmd.Option<bool>("-t|--types", Help.Command_ViewAll_Types, true);
            LinePrimitive<string> truncationIdentifier = cmd.Option<string>("-u|--truncate", Help.Command_ViewAll_Types, "...");

            cmd.OnExecute(() =>
            {

               ViewSettings settings = new ViewSettings
               {
                  displayMinWidth = displayMinWidth,
                  displayNulls = displayNulls,
                  displayTypes = displayTypes,
                  expandCells = expandCells,
                  truncationIdentifier = truncationIdentifier,
                  displayReferences = false
               };

               new DisplayFullCommand<Views.InteractiveConsoleView>(path).Execute(settings);
            });
         });

         int exitCode;
         using (L.Context(KnownProperty.OperationId, Guid.NewGuid().ToString()))
         {
            exitCode = app.Execute();
         }

         L.Config.Shutdown();

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

         L.Config
            .WriteTo.AzureApplicationInsights("0a310ae1-0f93-43fc-bfa1-62e92fc869b9")
            .EnrichWith.Constant(KnownProperty.Version, app.Version);

         Telemetry.CliInvoked(args);
      }
   }
}