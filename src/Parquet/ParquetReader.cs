/* MIT License
 *
 * Copyright (c) 2017 Elastacloud Limited
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

using Parquet.File;
using System;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;
using System.Collections;

namespace Parquet
{
   /// <summary>
   /// Implements Apache Parquet format reader
   /// </summary>
   public class ParquetReader : ParquetActor, IDisposable
   {
      private readonly Stream _input;
      private Thrift.FileMetaData _meta;
      private readonly ParquetOptions _formatOptions;
      private readonly ReaderOptions _readerOptions;

      /// <summary>
      /// Creates an instance from input stream
      /// </summary>
      /// <param name="input">Input stream, must be readable and seekable</param>
      /// <param name="formatOptions">Optional reader options</param>
      /// <param name="readerOptions">The reader options.</param>
      /// <exception cref="ArgumentNullException">input</exception>
      /// <exception cref="ArgumentException">stream must be readable and seekable - input</exception>
      /// <exception cref="IOException">not a Parquet file (size too small)</exception>
      public ParquetReader(Stream input, ParquetOptions formatOptions = null, ReaderOptions readerOptions = null) : base(input)
      {
         _input = input ?? throw new ArgumentNullException(nameof(input));
         if (!input.CanRead || !input.CanSeek) throw new ArgumentException("stream must be readable and seekable", nameof(input));
         if (_input.Length <= 8) throw new IOException("not a Parquet file (size too small)");

         ValidateFile();
         _formatOptions = formatOptions ?? new ParquetOptions();
         _readerOptions = readerOptions ?? new ReaderOptions();
      }

      /// <summary>
      /// Reads the file
      /// </summary>
      /// <param name="fullPath">The full path.</param>
      /// <param name="formatOptions">Optional reader options.</param>
      /// <param name="readerOptions">The reader options.</param>
      /// <returns><see cref="DataSet"/></returns>
      public static DataSet ReadFile(string fullPath, ParquetOptions formatOptions = null, ReaderOptions readerOptions = null)
      {
         using (Stream fs = System.IO.File.OpenRead(fullPath))
         {
            using (var reader = new ParquetReader(fs, formatOptions, readerOptions))
            {
               return reader.Read();
            }
         }
      }

      /// <summary>
      /// Reads <see cref="DataSet"/> from an open stream
      /// </summary>
      /// <param name="source">Input stream</param>
      /// <param name="formatOptions">Parquet options, optional.</param>
      /// <param name="readerOptions">Reader options, optional</param>
      /// <returns><see cref="DataSet"/></returns>
      public static DataSet Read(Stream source, ParquetOptions formatOptions = null, ReaderOptions readerOptions = null)
      {
         using (var reader = new ParquetReader(source, formatOptions, readerOptions))
         {
            return reader.Read();
         }
      }

      /// <summary>
      /// Options
      /// </summary>
      public ParquetOptions Options => _formatOptions;

      /// <summary>
      /// Total number of rows in this file
      /// </summary>
      public long RowCount => _meta.Num_rows;

      /// <summary>
      /// Test read, to be defined
      /// </summary>
      public DataSet Read()
      {
         _readerOptions.Validate();

         _meta = ReadMetadata();

         var metaParser = new FileMetadataParser(_meta);
         Schema schema = metaParser.ParseSchema(_formatOptions);

         if (schema.HasNestedElements) throw new NotSupportedException("nested structures are not yet supported");

         var pathToValues = new Dictionary<string, IList>();
         long pos = 0;
         long rowsRead = 0;

         foreach(Thrift.RowGroup rg in _meta.Row_groups)
         {
            //check whether to skip RG completely
            if ((_readerOptions.Count != -1 && rowsRead >= _readerOptions.Count) ||
               (_readerOptions.Offset > pos + rg.Num_rows - 1))
            {
               pos += rg.Num_rows;
               continue;
            }

            long offset = Math.Max(0, _readerOptions.Offset - pos);
            long count = _readerOptions.Count == -1 ? rg.Num_rows : Math.Min(_readerOptions.Count - rowsRead, rg.Num_rows);

            for(int icol = 0; icol < rg.Columns.Count; icol++)
            {
               Thrift.ColumnChunk cc = rg.Columns[icol];
               SchemaElement se = schema[cc];

               var p = new PColumn(cc, se, _input, ThriftStream, _formatOptions);

               try
               {
                  IList chunkValues = p.Read(offset, count);

                  if(!pathToValues.TryGetValue(se.Path, out IList allValues))
                  {
                     pathToValues[se.Path] = chunkValues;
                  }
                  else
                  {
                     allValues.AddRange(chunkValues);
                  }

                  if(icol == 0)
                  {
                     //todo: this may not work
                     rowsRead += chunkValues.Count;
                  }
               }
               catch(Exception ex)
               {
                  throw new ParquetException($"fatal error reading column '{se}'", ex);
               }
            }

            pos += rg.Num_rows;
         }

         var merger = new RecursiveMerge(schema);
         DataSet ds = merger.Merge(pathToValues);

         ds.TotalRowCount = _meta.Num_rows;
         ds.Metadata.CreatedBy = _meta.Created_by;

         return ds;
      }

      /// <summary>
      /// Disposes 
      /// </summary>
      public void Dispose()
      {
      }
   }
}