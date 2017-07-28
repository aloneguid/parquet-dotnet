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
      private readonly ParquetOptions _formatOptions;
      private readonly WriterOptions _writerOptions;
      private readonly SchemaElement _definitionsSchema = new SchemaElement<bool>("definitions");
      private readonly IValuesWriter _plainWriter;
      private readonly IValuesWriter _rleWriter;
      private readonly IValuesWriter _dicWriter;
      private bool _dataWritten;

      /// <summary>
      /// Creates an instance of parquet writer on top of a stream
      /// </summary>
      /// <param name="output">Writeable, seekable stream</param>
      /// <param name="formatOptions">Additional options</param>
      /// <param name="writerOptions">The writer options.</param>
      /// <exception cref="ArgumentNullException">Output is null.</exception>
      /// <exception cref="ArgumentException">Output stream is not writeable</exception>
      public ParquetWriter(Stream output, ParquetOptions formatOptions = null, WriterOptions writerOptions = null)
      {
         _output = output ?? throw new ArgumentNullException(nameof(output));
         if (!output.CanWrite) throw new ArgumentException("stream is not writeable", nameof(output));
         _thrift = new ThriftStream(output);
         _writer = new BinaryWriter(_output);
         _formatOptions = formatOptions ?? new ParquetOptions();
         _writerOptions = writerOptions ?? new WriterOptions();

         _plainWriter = new PlainValuesWriter(_formatOptions);
         _rleWriter = new RunLengthBitPackingHybridValuesWriter();
         _dicWriter = new PlainDictionaryValuesWriter();

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

         var stats = new DataSetStats(dataSet);

         //long totalCount = dataSet.Count;

         int offset = 0;
         int count;
         do
         {
            count = Math.Min(_writerOptions.RowGroupsSize, dataSet.Count - offset);
            Thrift.RowGroup rg = _meta.AddRowGroup();
            long rgStartPos = _output.Position;
            rg.Columns = dataSet.Schema.Elements
               .Select(c =>
                  Write(c, dataSet.GetColumn(c.Name, offset, count), compression, stats.GetColumnStats(c)))
               .ToList();

            //row group's size is a sum of _uncompressed_ sizes of all columns in it
            rg.Total_byte_size = rg.Columns.Sum(c => c.Meta_data.Total_uncompressed_size);
            rg.Num_rows = count;

            offset += _writerOptions.RowGroupsSize;
         }
         while (offset < dataSet.Count);

         _dataWritten = true;
      }

      public static void Write(DataSet dataSet, Stream destination, CompressionMethod compression = CompressionMethod.Gzip, ParquetOptions formatOptions = null, WriterOptions writerOptions = null)
      {
         using (var writer = new ParquetWriter(destination, formatOptions, writerOptions))
         {
            writer.Write(dataSet, compression);
         }
      }

      public static void WriteFile(DataSet ds, string fileName, CompressionMethod compression = CompressionMethod.Gzip, ParquetOptions formatOptions = null, WriterOptions writerOptions = null)
      {
         using (Stream fs = System.IO.File.Create(fileName))
         {
            using (var writer = new ParquetWriter(fs, formatOptions, writerOptions))
            {
               writer.Write(ds, compression);
            }
         }
      }

      private Thrift.ColumnChunk Write(SchemaElement schema, IList values,
         CompressionMethod compression,
         ColumnStats stats)
      {
         Thrift.ColumnChunk chunk = _meta.AddColumnChunk(compression, _output, schema, values.Count);
         Thrift.PageHeader ph = _meta.CreateDataPage(values.Count);

         WriteValues(schema, values, ph, compression, stats);

         return chunk;
      }

      private void WriteValues(SchemaElement schema, IList values, Thrift.PageHeader ph, CompressionMethod compression, ColumnStats stats)
      {
         byte[] dictionaryPageBytes = null;
         byte[] dataPageBytes;

         using (var ms = new MemoryStream())
         {
            using (var writer = new BinaryWriter(ms))
            {
               //write definitions
               if(stats.NullCount > 0)
               {
                  CreateDefinitions(values, schema, out IList newValues, out List<int> definitions);
                  values = newValues;

                  _rleWriter.Write(writer, _definitionsSchema, definitions, out IList nullExtra);
               }

               //write data
               if (!_dicWriter.Write(writer, schema, values, out IList dicValues))
               {
                  _plainWriter.Write(writer, schema, values, out IList plainExtra);
               }
               else
               {
                  ph.Data_page_header.Encoding = Thrift.Encoding.PLAIN_DICTIONARY;
                  using (var dms = new MemoryStream())
                     using(var dwriter = new BinaryWriter(dms))
                  {
                     _plainWriter.Write(dwriter, schema, dicValues, out IList t0);
                     dictionaryPageBytes = dms.ToArray();
                  }
               }

               dataPageBytes = ms.ToArray();
            }
         }

         if(dictionaryPageBytes != null)
         {
            Thrift.PageHeader dph = _meta.CreateDictionaryPage(values.Count);
            dictionaryPageBytes = Compress(dph, dictionaryPageBytes, compression);
            Write(dph, dictionaryPageBytes);
         }

         dataPageBytes = Compress(ph, dataPageBytes, compression);
         Write(ph, dataPageBytes);
      }

      private static void CreateDefinitions(IList values, SchemaElement schema, out IList nonNullableValues, out List<int> definitions)
      {
         nonNullableValues = TypeFactory.Create(schema, false);
         definitions = new List<int>();

         foreach(var value in values)
         {
            if(value == null)
            {
               definitions.Add(0);
            }
            else
            {
               definitions.Add(1);
               nonNullableValues.Add(value);
            }
         }
      }

      private void Write(Thrift.PageHeader ph, byte[] data)
      {
         _thrift.Write(ph);
         _output.Write(data, 0, data.Length);
      }

      private byte[] Compress(Thrift.PageHeader ph, byte[] data, CompressionMethod compression)
      {
         ph.Uncompressed_page_size = data.Length;
         byte[] result;

         if (compression != CompressionMethod.None)
         {
            IDataWriter writer = DataFactory.GetWriter(compression);
            using (var ms = new MemoryStream())
            {
               writer.Write(data, ms);
               result = ms.ToArray();
            }
            ph.Compressed_page_size = result.Length;
         }
         else
         {
            ph.Compressed_page_size = ph.Uncompressed_page_size;
            result = data;
         }

         return result;
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
         if (!_dataWritten) return;

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
