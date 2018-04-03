using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.Data;
using Parquet.File;
using Parquet.Serialization;

namespace Parquet
{
   /// <summary>
   /// v3 experimental!
   /// </summary>
   internal static class ParquetConvert
   {
      public static Schema Serialize<T>(IEnumerable<T> objectInstances, Stream destination,
         Schema schema = null,
         WriterOptions writerOptions = null,
         CompressionMethod compressionMethod = CompressionMethod.Snappy)
      {
         if (objectInstances == null) throw new ArgumentNullException(nameof(objectInstances));
         if (destination == null) throw new ArgumentNullException(nameof(destination));
         if (!destination.CanWrite) throw new ArgumentException("stream must be writeable", nameof(destination));

         //if schema is not passed reflect it
         if (schema == null)
         {
            schema = SchemaReflector.Reflect<T>();
         }

         if (writerOptions == null) writerOptions = new WriterOptions();

         var extractor = new ColumnExtractor();
         using (var writer = new ParquetWriter3(schema, destination, writerOptions: writerOptions))
         {
            writer.CompressionMethod = compressionMethod;

            foreach(IEnumerable<T> batch in objectInstances.Batch(writerOptions.RowGroupsSize))
            {
               List<DataColumn> columns = extractor.ExtractColumns(batch, schema);

               using (ParquetRowGroupWriter groupWriter = writer.CreateRowGroup(batch.Count()))
               {
                  foreach(DataColumn dataColumn in columns)
                  {
                     groupWriter.Write(dataColumn);
                  }
               }
            }
         }

         return schema;
      }
   }
}
