using System;
using LogMagic;
using Parquet.Data;
using static Cpf.PoshConsole;

namespace Parquet.CLI.Commands
{
   class SchemaCommand
   {
      private static readonly ILog log = L.G(typeof(SchemaCommand));
      private readonly string _path;

      public SchemaCommand(string path)
      {
         _path = path ?? throw new ArgumentNullException(nameof(path));
      }

      public void Execute()
      {
         Telemetry.CommandExecuted("schema",
            "path", _path);

         using (var time = new TimeMeasure())
         {
            using (var reader = ParquetReader.OpenFromFile(_path))
            {
               Schema schema = reader.Schema;

               PrintSchema(schema, time.Elapsed);
            }
         }
      }

      private void PrintSchema(Schema schema, TimeSpan timeTaken)
      {
         foreach(Field field in schema.Fields)
         {
            PrintField(field, 0);
         }

         WriteLine();
         Write("reading schema took ", ConsoleColor.DarkGray);
         Write(timeTaken.TotalMilliseconds.ToString(), ConsoleColor.Red);
         Write("ms", ConsoleColor.DarkGray);
         WriteLine();
      }

      private void PrintField(Field field, int nesting)
      {
         if(nesting > 0)
         {
            Write(new string('.', nesting * 2), ConsoleColor.DarkGray);
         }

         Write(field.Name, ConsoleColor.Green);

         switch (field.SchemaType)
         {
            case SchemaType.Data:
               var df = (DataField)field;
               if(df.HasNulls)
               {
                  Write("?", ConsoleColor.White);
               }
               Write(" ");
               Write(df.DataType.ToString(), ConsoleColor.Red);
               if (df.IsArray)
               {
                  Write("[]", ConsoleColor.Yellow);
               }
               Write(" ");
               Write(df.Path, ConsoleColor.DarkGray);
               //WriteBasics(field);
               
               WriteLine();
               break;

            case SchemaType.Map:
               var mf = (MapField)field;
               Write(" (map)", ConsoleColor.Yellow);
               WriteLine();
               PrintField(mf.Key, nesting + 1);
               PrintField(mf.Value, nesting + 1);
               break;

            case SchemaType.Struct:
               var sf = (StructField)field;
               Write(" (struct)", ConsoleColor.Yellow);
               WriteLine();
               foreach(Field f in sf.Fields)
               {
                  PrintField(f, nesting + 1);
               }
               break;

            case SchemaType.List:
               var lf = (ListField)field;
               Write(" (list)", ConsoleColor.Yellow);
               WriteLine();
               PrintField(lf.Item, nesting + 1);
               break;

            default:
               WriteLine(field.SchemaType.ToString() + "?", ConsoleColor.Yellow);
               break;
         }
      }

      private void WriteBasics(Field f)
      {
         PoshWrite($" {{RL}}{{{f.MaxRepetitionLevel}}} {{DL}}{{{f.MaxDefinitionLevel}}}",
            ConsoleColor.DarkGray, T.ActiveTextColor,
            ConsoleColor.DarkGray, T.ActiveTextColor);
      }
   }
}
