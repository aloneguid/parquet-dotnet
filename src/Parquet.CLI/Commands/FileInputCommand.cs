using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using NetBox.Terminal.Widgets;
using Parquet.Data.Rows;
using Table = Parquet.Data.Rows.Table;

namespace Parquet.CLI.Commands
{
   abstract class FileInputCommand
   {
      private readonly string _path;

      public FileInputCommand(string path)
      {
         _path = path;
      }

      protected async Task<Table> ReadTableAsync(int maxRows = 10)
      {
         using (var msg = new ProgressMessage($"reading file ({maxRows} rows min)..."))
         {
            try
            {
               await using (ParquetReader reader = await ParquetReader.OpenFromFileAsync(_path, new ParquetOptions { TreatByteArrayAsString = true }))
               {
                  Table table = await reader.ReadAsTableAsync().ConfigureAwait(false);

                  return table;
               }
            }
            catch(Exception ex)
            {
               msg.Fail(ex.Message);
               throw;
            }
         }
      }

      protected async Task<Thrift.FileMetaData> ReadInternalMetadataAsync()
      {
         await using (ParquetReader reader = await ParquetReader.OpenFromFileAsync(_path))
         {
            return reader.ThriftMetadata;
         }
      }
   }
}