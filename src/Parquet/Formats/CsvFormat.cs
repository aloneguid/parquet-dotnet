using NetBox.FileFormats;
using Parquet.Data;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Parquet.Formats
{
   /// <summary>
   /// CSV support for DataSet
   /// </summary>
   public static class CsvFormat
   {
      private static readonly Dictionary<Type, Type> InferredTypeToParquetType = new Dictionary<Type, Type>
      {
         { typeof(byte), typeof(int) }
      };
      
      /// <summary>
      /// Reads csv stream into dataset
      /// </summary>
      /// <param name="csvStream">CSV stream</param>
      /// <param name="options">Options for reader, optional</param>
      /// <returns>Correct dataset</returns>
      public static DataSet ReadToDataSet(Stream csvStream, CsvOptions options = null)
      {
         if (csvStream == null) throw new ArgumentNullException(nameof(csvStream));

         if (options == null) options = new CsvOptions();

         var reader = new CsvReader(csvStream, Encoding.UTF8);

         string[] headers = null;
         var columnValues = new Dictionary<int, IList>();

         string[] values;
         while((values = reader.ReadNextRow()) != null)
         {
            //set headers
            if(headers == null)
            {
               if(options.HasHeaders)
               {
                  headers = values;
                  continue;
               }
               else
               {
                  headers = new string[values.Length];
                  for(int i = 0; i < values.Length; i++)
                  {
                     headers[i] = $"Col{i}";
                  }
               }
            }

            //get values
            for(int i = 0; i < values.Length; i++)
            {
               if(!columnValues.TryGetValue(i, out IList col))
               {
                  col = new List<string>();
                  columnValues[i] = col;
               }

               col.Add(values[i]);
            }
         }

         DataSet result;

         //set schema
         if (options.InferSchema)
         {
            result = InferSchema(headers, columnValues);
         }
         else
         {
            result = new DataSet(headers.Select(h => new SchemaElement<string>(h)));
         }

         //assign values
         result.AddColumnar(columnValues.Values);
         return result;
      }

      /// <summary>
      /// Reads csv stream into dataset
      /// </summary>
      /// <param name="fileName">File on disk to read from/param>
      /// <param name="options">Options for reader, optional</param>
      /// <returns>Correct dataset</returns>
      public static DataSet ReadToDataSet(string fileName, CsvOptions options = null)
      {
         if (fileName == null) throw new ArgumentNullException(nameof(fileName));

         using (Stream fs = System.IO.File.OpenRead(fileName))
         {
            return ReadToDataSet(fs, options);
         }
      }

      private static DataSet InferSchema(string[] headers, Dictionary<int, IList> columnValues)
      {
         var elements = new List<SchemaElement>();
         for (int i = 0; i < headers.Length; i++)
         {
            IList cv = columnValues[i];
            Type columnType = cv.Cast<string>().ToArray().InferType(out IList typedValues);

            Type ct;
            if (!InferredTypeToParquetType.TryGetValue(columnType, out ct)) ct = columnType;
            elements.Add(new SchemaElement(headers[i], ct));

            columnValues[i] = typedValues;
         }

         return new DataSet(elements);
      }
   }
}
