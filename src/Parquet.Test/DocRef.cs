using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;

namespace Parquet.Test
{
   class DocRef
   {
      public async Task ReadIntroAsync()
      {
         // open file stream
         using (Stream fileStream = System.IO.File.OpenRead("c:\\test.parquet"))
         {
            // open parquet file reader
            await using (var parquetReader = new ParquetReader(fileStream))
            {
               // get file schema (available straight after opening parquet reader)
               // however, get only data fields as only they contain data values
               DataField[] dataFields = parquetReader.Schema.GetDataFields();

               // enumerate through row groups in this file
               for (int i = 0; i < parquetReader.RowGroupCount; i++)
               {
                  // create row group reader
                  using (ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(i))
                  {
                     // read all columns inside each row group (you have an option to read only
                     // required columns if you need to.
                     DataColumn[] columns = new DataColumn[dataFields.Length];

                     for (int j = 0; j < dataFields.Length; j++)
                     {
                        columns[j] = await groupReader.ReadColumnAsync(dataFields[j]).ConfigureAwait(false);
                     }

                     // get first column, for instance
                     DataColumn firstColumn = columns[0];

                     // .Data member contains a typed array of column data you can cast to the type of the column
                     Array data = firstColumn.Data;
                     int[] ids = (int[])data;
                  }
               }
            }
         }
      }

      public async Task WriteIntroAsync()
      {
         //create data columns with schema metadata and the data you need
         var idColumn = new DataColumn(
            new DataField<int>("id"),
            new int[] { 1, 2 });

         var cityColumn = new DataColumn(
            new DataField<string>("city"),
            new string[] { "London", "Derby" });

         // create file schema
         var schema = new Schema(idColumn.Field, cityColumn.Field);

         using (Stream fileStream = System.IO.File.OpenWrite("c:\\test.parquet"))
         {
            await using (var parquetWriter = new ParquetWriter(schema, fileStream))
            {
               // create a new row group in the file
               using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
               {
                  await groupWriter.WriteColumnAsync(idColumn).ConfigureAwait(false);
                  await groupWriter.WriteColumnAsync(cityColumn).ConfigureAwait(false);
               }
            }
         }
      }


      //
   }
}
