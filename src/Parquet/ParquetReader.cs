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
   public class ParquetReader : IDisposable
   {
      private readonly Stream _input;
      private readonly BinaryReader _reader;
      private readonly ThriftStream _thrift;
      private Thrift.FileMetaData _meta;
      private readonly ParquetOptions _options;

      /// <summary>
      /// Creates an instance from input stream
      /// </summary>
      /// <param name="input">Input stream, must be readable and seekable</param>
      /// <param name="options">Optional reader options</param>
      public ParquetReader(Stream input, ParquetOptions options = null)
      {
         _input = input ?? throw new ArgumentNullException(nameof(input));
         if (!input.CanRead || !input.CanSeek) throw new ArgumentException("stream must be readable and seekable", nameof(input));
         if (_input.Length <= 8) throw new IOException("not a Parquet file (size too small)");

         _reader = new BinaryReader(input);
         ValidateFile();
         _thrift = new ThriftStream(input);
         _options = options ?? new ParquetOptions();
      }

      /// <summary>
      /// Reads the file
      /// </summary>
      /// <param name="fullPath">The full path.</param>
      /// <param name="options">Optional reader options.</param>
      /// <returns>DataSet</returns>
      public static DataSet ReadFile(string fullPath, ParquetOptions options = null)
      {
         using (Stream fs = System.IO.File.OpenRead(fullPath))
         {
            using (var reader = new ParquetReader(fs, options))
            {
               return reader.Read();
            }
         }
      }

      public static DataSet Read(Stream source, ParquetOptions options = null)
      {
         using (var reader = new ParquetReader(source, options))
         {
            return reader.Read();
         }
      }

      /// <summary>
      /// Options
      /// </summary>
      public ParquetOptions Options => _options;

      /// <summary>
      /// Total number of rows in this file
      /// </summary>
      public long RowCount => _meta.Num_rows;

      /// <summary>
      /// Test read, to be defined
      /// </summary>
      public DataSet Read()
      {
         _meta = ReadMetadata();
         var ds = new DataSet(new Schema(_meta));
         var cols = new List<IList>();

         foreach(Thrift.RowGroup rg in _meta.Row_groups)
         {
            foreach(Thrift.ColumnChunk cc in rg.Columns)
            {
               var p = new PColumn(cc, ds.Schema, _input, _thrift, _options);
               string columnName = string.Join(".", cc.Meta_data.Path_in_schema);

               try
               {
                  IList column = p.Read(columnName);
                  cols.Add(column);
               }
               catch(Exception ex)
               {
                  throw new ParquetException($"fatal error reading column '{columnName}'", ex);
               }
            }
         }

         ds.AddColumnar(cols);

         return ds;
      }

      private void ValidateFile()
      {
         _input.Seek(0, SeekOrigin.Begin);
         char[] head = _reader.ReadChars(4);
         string shead = new string(head);
         _input.Seek(-4, SeekOrigin.End);
         char[] tail = _reader.ReadChars(4);
         string stail = new string(tail);
         if (shead != "PAR1")
            throw new IOException($"not a Parquet file(head is '{shead}')");
         if (stail != "PAR1")
            throw new IOException($"not a Parquet file(head is '{stail}')");
      }

      private Thrift.FileMetaData ReadMetadata()
      {
         //go to -4 bytes (PAR1) -4 bytes (footer length number)
         _input.Seek(-8, SeekOrigin.End);
         int footerLength = _reader.ReadInt32();
         char[] magic = _reader.ReadChars(4);

         //go to footer data and deserialize it
         _input.Seek(-8 - footerLength, SeekOrigin.End);
         return _thrift.Read<Thrift.FileMetaData>();
      }

      /// <summary>
      /// Disposes 
      /// </summary>
      public void Dispose()
      {
      }
   }
}