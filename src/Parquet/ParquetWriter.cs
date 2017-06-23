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

using System;
using System.IO;
using System.Text;
using Parquet.Thrift;
using Parquet.File;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Parquet.File.Values;

namespace Parquet
{
   /// <summary>
   /// Implements Apache Parquet format writer
   /// </summary>
   public class ParquetWriter : IDisposable
   {
      private readonly Stream _output;
      private readonly BinaryWriter _writer;
      private readonly ThriftStream _thrift;
      private static readonly byte[] Magic = System.Text.Encoding.ASCII.GetBytes("PAR1");
      private readonly FileMetaData _meta = new FileMetaData();
      private readonly IValuesWriter _plainWriter = new PlainValuesWriter();

      public ParquetWriter(Stream output)
      {
         _output = output ?? throw new ArgumentNullException(nameof(output));
         if (!output.CanWrite) throw new ArgumentException("stream is not writeable", nameof(output));
         _thrift = new ThriftStream(output);
         _writer = new BinaryWriter(_output);

         //file starts with magic
         WriteMagic();

         _meta.Created_by = "parquet-dotnet";
         _meta.Version = 1;
         _meta.Row_groups = new List<RowGroup>();
      }

      public void Write(ParquetDataSet dataSet, int rowGroupSize = 5000)
      {
         long totalCount = dataSet.Count;

         _meta.Schema = new List<SchemaElement>(dataSet.Columns.Select(c => c.Schema));
         _meta.Num_rows = totalCount;

         //write in row groups

         for(long rowIdx = 0; rowIdx < totalCount; rowIdx += rowGroupSize)
         {
            var thriftRowGroup = new RowGroup();
            thriftRowGroup.Columns = new List<ColumnChunk>();

            foreach (ParquetColumn column in dataSet.Columns)
            {
               ColumnChunk thriftChunk = Write(column, rowIdx, rowGroupSize);
               thriftRowGroup.Columns.Add(thriftChunk);
            }

            _meta.Row_groups.Add(thriftRowGroup);
         }

         _thrift.Write(_meta);
      }

      private ColumnChunk Write(ParquetColumn column, long rowIdx, long groupSize)
      {
         var chunk = new ColumnChunk();
         long startPos = _output.Position;
         chunk.File_offset = startPos;
         chunk.Meta_data = new ColumnMetaData();
         chunk.Meta_data.Type = column.Type;
         chunk.Meta_data.Codec = CompressionCodec.UNCOMPRESSED;   //todo: compression should be passed as parameter
         chunk.Meta_data.Data_page_offset = startPos;

         /*_plainWriter.Write(_writer, column.Schema,
            column.Values.OfType<object>().Skip((int)rowIdx).Take((int)groupSize).ToList()); //todo: heavy, must be rewritten

         long endPos = _output.Position;
         chunk.Meta_data.Total_compressed_size = chunk.Meta_data.Total_uncompressed_size = endPos - startPos;
         */

         return chunk;
      }

      private void WriteMagic()
      {
         _output.Write(Magic, 0, Magic.Length);
      }

      public void Dispose()
      {
         //finalize file

         //write thrift metadata
         long pos = _output.Position;
         //_output.ThriftWrite(_meta);
         long size = _output.Position - pos;

         //metadata size
         _writer.Write((int)size);  //4 bytes

         //end magic
         WriteMagic();              //4 bytes
      }
   }
}
