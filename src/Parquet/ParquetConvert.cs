using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.Data;
using Parquet.File;
using Parquet.Serialization;
using Parquet.Serialization.Values;

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
         where T : new()
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

         var extractor = new DataColumnBuilder();
         using (var writer = new ParquetWriter(schema, destination, writerOptions: writerOptions))
         {
            writer.CompressionMethod = compressionMethod;

            foreach(IEnumerable<T> batch in objectInstances.Batch(writerOptions.RowGroupsSize))
            {
               IReadOnlyCollection<DataColumn> columns = extractor.BuildColumns(batch.ToList(), schema);

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

      public static IEnumerable<T> Deserialize<T>(Stream input) where T : new()
      {
         var result = new List<T>();

         var bridge = new ClrBridge(typeof(T));

         using (var reader = new ParquetReader(input))
         {
            Schema fileSchema = reader.Schema;
            List<DataField> dataFields = fileSchema.GetDataFields();

            for(int i = 0; i < reader.RowGroupCount; i++)
            {
               using (ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(i))
               {
                  List<DataColumn> groupColumns = dataFields
                     .Select(df => groupReader.ReadColumn(df))
                     .ToList();

                  IReadOnlyCollection<T> groupClrObjects = bridge.CreateClassInstances<T>(groupColumns);

                  result.AddRange(groupClrObjects);
               }
            }
         }

         return result;
      }
   }
}
