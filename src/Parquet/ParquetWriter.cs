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
using Parquet.Thrift;
using Parquet.File;
using System.Collections.Generic;
using System.Linq;
using Parquet.File.Values;
using TEncoding = Parquet.Thrift.Encoding;
using Parquet.File.Data;

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
      private readonly MetaBuilder _meta = new MetaBuilder();
      private readonly ParquetOptions _options = new ParquetOptions();
      private readonly IValuesWriter _plainWriter;
      private IDataWriter _dataWriter;

      /// <summary>
      /// Creates an instance of parquet writer on top of a stream
      /// </summary>
      /// <param name="output">Writeable, seekable stream</param>
      public ParquetWriter(Stream output)
      {
         _output = output ?? throw new ArgumentNullException(nameof(output));
         if (!output.CanWrite) throw new ArgumentException("stream is not writeable", nameof(output));
         _thrift = new ThriftStream(output);
         _writer = new BinaryWriter(_output);

         _plainWriter = new PlainValuesWriter(_options);

         //file starts with magic
         WriteMagic();
      }

      /// <summary>
      /// Write out dataset to the output stream
      /// </summary>
      /// <param name="dataSet">Dataset to write</param>
      /// <param name="compression">Compression method</param>
      public void Write(ParquetDataSet dataSet, CompressionMethod compression = CompressionMethod.None)
      {
         _dataWriter = CreateDataWriter(compression);

         _meta.AddSchema(dataSet);

         long totalCount = dataSet.Count;

         RowGroup rg = _meta.AddRowGroup();
         long rgStartPos = _output.Position;
         rg.Columns = dataSet.Columns.Select(c => Write(c)).ToList();

         //row group's size is a sum of _uncompressed_ sizes of all columns in it
         rg.Total_byte_size = rg.Columns.Sum(c => c.Meta_data.Total_uncompressed_size);
         rg.Num_rows = dataSet.Count;
      }

      private IDataWriter CreateDataWriter(CompressionMethod method)
      {
         switch(method)
         {
            case CompressionMethod.None:
               return new UncompressedDataWriter(_output);

            default:
               throw new NotImplementedException($"method {method} is not supported");
         }
      }

      private ColumnChunk Write(ParquetColumn column)
      {
         var chunk = new ColumnChunk();
         long startPos = _output.Position;
         chunk.File_offset = startPos;
         chunk.Meta_data = new ColumnMetaData();
         chunk.Meta_data.Num_values = column.Values.Count;
         chunk.Meta_data.Type = column.Type;
         chunk.Meta_data.Codec = CompressionCodec.UNCOMPRESSED;   //todo: compression should be passed as parameter
         chunk.Meta_data.Data_page_offset = startPos;
         chunk.Meta_data.Encodings = new List<TEncoding>
         {
            TEncoding.PLAIN
         };
         chunk.Meta_data.Path_in_schema = new List<string> { column.Name };

         var ph = new PageHeader(PageType.DATA_PAGE, 0, 0);
         ph.Data_page_header = new DataPageHeader
         {
            Encoding = TEncoding.PLAIN,
            Definition_level_encoding = TEncoding.RLE,
            Repetition_level_encoding = TEncoding.BIT_PACKED,
            Num_values = column.Values.Count
         };

         WriteValues(column, ph);

         return chunk;
      }

      private void WriteValues(ParquetColumn column, PageHeader ph)
      {
         using (var ms = new MemoryStream())
         {
            using (var columnWriter = new BinaryWriter(ms))
            {
               //columnWriter.Write((int)0);   //definition levels, only for nullable columns
               _plainWriter.Write(columnWriter, column.Schema, column.Values);

               //

               ph.Compressed_page_size = ph.Uncompressed_page_size = (int)ms.Length;
               byte[] data = ms.ToArray();
               _thrift.Write(ph);
               _dataWriter.Write(data);
            }
         }
      }

      private void WriteMagic()
      {
         _output.Write(Magic, 0, Magic.Length);
      }

      /// <summary>
      /// Finalizes file, writes metadata and footer
      /// </summary>
      public void Dispose()
      {
         //finalize file
         long size = _thrift.Write(_meta.ThriftMeta);

         //metadata size
         _writer.Write((int)size);  //4 bytes

         //end magic
         WriteMagic();              //4 bytes

         _writer.Flush();
         _output.Flush();
      }
   }
}
