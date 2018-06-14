using System;
using System.IO;
using System.Threading.Tasks;
using LogMagic;
using Storage.Net;
using Storage.Net.Blob;
using NetBox.Extensions;
using System.Threading;
using System.Collections.Generic;
using NetBox.FileFormats;
using System.Linq;
using Newtonsoft.Json.Linq;
using Parquet.Data;
using Parquet;
using Parquet.Json;

namespace Demo.DropFolder
{
   class Program
   {
      private static readonly ILog log = L.G(typeof(Program));

      async static Task Main(string[] args)
      {
         L.Config.WriteTo.PoshConsole();

         IBlobStorage sourceStorage = StorageFactory.Blobs.DirectoryFiles(new DirectoryInfo(args[0]));
         IBlobStorage targetStorage = StorageFactory.Blobs.DirectoryFiles(new DirectoryInfo(args[1]));

         log.Trace("monitoring {0}...", args[0]);

         var cts = new CancellationTokenSource();

         await MonitorAsync(sourceStorage, targetStorage, cts.Token);

         Console.ReadLine();
         cts.Cancel();
      }

      private static async Task MonitorAsync(IBlobStorage input, IBlobStorage output, CancellationToken cancellationToken)
      {
         while(!cancellationToken.IsCancellationRequested)
         {
            IEnumerable<BlobId> inputFiles = await input.ListFilesAsync(null, cancellationToken);

            foreach(BlobId blob in inputFiles)
            {
               await ConvertAsync(input, output, blob.FullPath, cancellationToken);

               await input.DeleteAsync(blob.FullPath, cancellationToken);
            }

            await Task.Delay(TimeSpan.FromSeconds(1));
         }
      }

      private static async Task ConvertAsync(IBlobStorage input, IBlobStorage output, string blobId, CancellationToken cancellationToken)
      {
         BlobMeta inputMeta = (await input.GetMetaAsync(new[] { blobId })).First();

         log.Trace("converting {0}...", blobId);

         DataSet ds = null;

         log.Trace("reading json...");
         using (Stream inputStream = await input.OpenReadAsync(blobId))
         {
            using (var reader = new StreamReader(inputStream))
            {
               string s;
               JsonDataExtractor extractor = null;
               while((s = reader.ReadLine()) != null)
               {
                  JObject jo = JObject.Parse(s);

                  if(ds == null)
                  {
                     Schema schema = jo.InferParquetSchema();
                     ds = new DataSet(schema);
                     extractor = new JsonDataExtractor(schema);
                  }

                  extractor.AddRow(ds, jo);
               }
            }
         }

         log.Trace("writing output file...");
         using (var ms = new MemoryStream())
         {
            ParquetWriter.Write(ds, ms);
            ms.Position = 0;

            await output.WriteAsync(blobId + ".parquet", ms);

            log.Trace("converted to parquet, output size: {0}", ms.Length.ToFileSizeUiString());
         }

         //using (Stream inputStream = await input.OpenReadAsync(blobId, cancellationToken))
         //{
         //   var reader = new CsvReader(inputStream, )
      }
   }
}