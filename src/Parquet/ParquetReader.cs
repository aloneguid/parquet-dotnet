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
using System.Text;
using Parquet.Thrift;

namespace Parquet
{
   /// <summary>
   /// Implements Apache Parquet format reader
   /// </summary>
   public class ParquetReader : IDisposable
   {
      private readonly Stream _input;
      private readonly BinaryReader _reader;
      private readonly FileMetaData _meta;
      private readonly Schema _schema;

      public ParquetReader(Stream input)
      {
         _input = input;
         _reader = new BinaryReader(input);

         _meta = ReadMetadata();
         _schema = new Schema(_meta);
      }

      /// <summary>
      /// Test read, to be defined
      /// </summary>
      public ParquetDataSet Read()
      {
         var result = new List<ParquetColumn>();

         foreach(RowGroup rg in _meta.Row_groups)
         {
            foreach(ColumnChunk cc in rg.Columns)
            {
               var p = new PColumn(cc, _schema, _input);
               ParquetColumn column = p.Read();
               result.Add(column);
            }
         }

         return new ParquetDataSet(result);
      }

      private FileMetaData ReadMetadata()
      {
         //todo: validation that it's a parquet format indeed

         //go to -4 bytes (PAR1) -4 bytes (footer length number)
         _input.Seek(-8, SeekOrigin.End);
         int footerLength = _reader.ReadInt32();
         char[] magic = _reader.ReadChars(4);

         //go to footer data and deserialize it
         _input.Seek(-8 - footerLength, SeekOrigin.End);
         return _input.ThriftRead<FileMetaData>();
      }

      public void Dispose()
      {
      }
   }
}