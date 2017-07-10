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
using Parquet.File;
using System.Collections.Generic;
using System.Linq;
using Parquet.File.Values;
using Parquet.File.Data;
using System.Collections;
using Parquet.Data;

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
      public void Write(DataSet dataSet, CompressionMethod compression = CompressionMethod.Gzip)
      {
         _meta.AddSchema(dataSet);

         long totalCount = dataSet.Count;

         Thrift.RowGroup rg = _meta.AddRowGroup();
         long rgStartPos = _output.Position;
         rg.Columns = dataSet.Schema.Elements.Select(c => Write(c, dataSet.GetColumn(c.Name), compression)).ToList();

         //row group's size is a sum of _uncompressed_ sizes of all columns in it
         rg.Total_byte_size = rg.Columns.Sum(c => c.Meta_data.Total_uncompressed_size);
         rg.Num_rows = dataSet.Count;
      }

      private Thrift.ColumnChunk Write(SchemaElement schema, IList values, CompressionMethod compression)
      {
         Thrift.CompressionCodec codec = DataFactory.GetThriftCompression(compression);

         var chunk = new Thrift.ColumnChunk();
         long startPos = _output.Position;
         chunk.File_offset = startPos;
         chunk.Meta_data = new Thrift.ColumnMetaData();
         chunk.Meta_data.Num_values = values.Count;
         chunk.Meta_data.Type = schema.Thrift.Type;
         chunk.Meta_data.Codec = codec;
         chunk.Meta_data.Data_page_offset = startPos;
         chunk.Meta_data.Encodings = new List<Thrift.Encoding>
         {
            Thrift.Encoding.PLAIN
         };
         chunk.Meta_data.Path_in_schema = new List<string> { schema.Name };

         var ph = new Thrift.PageHeader(Thrift.PageType.DATA_PAGE, 0, 0);
         ph.Data_page_header = new Thrift.DataPageHeader
         {
            Encoding = Thrift.Encoding.PLAIN,
            Definition_level_encoding = Thrift.Encoding.RLE,
            Repetition_level_encoding = Thrift.Encoding.BIT_PACKED,
            Num_values = values.Count
         };

         WriteValues(schema, values, ph, compression);

         return chunk;
      }

      private void WriteValues(SchemaElement schema, IList values, Thrift.PageHeader ph, CompressionMethod compression)
      {
         byte[] data;

         using (var ms = new MemoryStream())
         {
            using (var writer = new BinaryWriter(ms))
            {
               //columnWriter.Write((int)0);   //definition levels, only for nullable columns
               _plainWriter.Write(writer, schema, values);

               data = ms.ToArray();
            }
         }

         ph.Uncompressed_page_size = data.Length;

         if(compression != CompressionMethod.None)
         {
            IDataWriter writer = DataFactory.GetWriter(compression);
            using (var ms = new MemoryStream())
            {
               writer.Write(data, ms);
               data = ms.ToArray();
            }
            ph.Compressed_page_size = data.Length;
         }
         else
         {
            ph.Compressed_page_size = ph.Uncompressed_page_size;
         }

         _thrift.Write(ph);
         _output.Write(data, 0, data.Length);
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
