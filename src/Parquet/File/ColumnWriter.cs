using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.Data;
using Parquet.File.Data;
using Parquet.File.Values;

namespace Parquet.File
{
   class ColumnWriter
   {
      private readonly Stream _output;
      private readonly ThriftStream _thriftStream;
      private readonly FileMetadataBuilder _meta;
      private readonly SchemaElement _schema;
      private readonly CompressionMethod _compressionMethod;
      private readonly ParquetOptions _formatOptions;
      private readonly WriterOptions _writerOptions;
      private readonly IValuesWriter _plainWriter;
      private readonly IValuesWriter _rleWriter;
      private readonly IValuesWriter _dicWriter;

      private struct PageTag
      {
         public int HeaderSize;
         public Thrift.PageHeader HeaderMeta;
      }

      public ColumnWriter(Stream output, ThriftStream thriftStream,
         FileMetadataBuilder builder, SchemaElement schema,
         CompressionMethod compressionMethod,
         ParquetOptions formatOptions,
         WriterOptions writerOptions)
      {
         _output = output;
         _thriftStream = thriftStream;
         _meta = builder;
         _schema = schema;
         _compressionMethod = compressionMethod;
         _formatOptions = formatOptions;
         _writerOptions = writerOptions;
         _plainWriter = new PlainValuesWriter(formatOptions);
         _rleWriter = new RunLengthBitPackingHybridValuesWriter();
         _dicWriter = new PlainDictionaryValuesWriter(_rleWriter);
      }

      public Thrift.ColumnChunk Write(int offset, int count, IList values)
      {
         if (values == null) values = TypeFactory.Create(_schema.ElementType, _schema.IsNullable, _schema.IsRepeated);

         Thrift.ColumnChunk chunk = _meta.AddColumnChunk(_compressionMethod, _output, _schema, values.Count);
         Thrift.PageHeader ph = _meta.CreateDataPage(values.Count);

         List<PageTag> pages = WriteValues(_schema, values, ph, _compressionMethod);

         chunk.Meta_data.Num_values = ph.Data_page_header.Num_values;

         //the following counters must include both data size and header size
         chunk.Meta_data.Total_compressed_size = pages.Sum(p => p.HeaderMeta.Compressed_page_size + p.HeaderSize);
         chunk.Meta_data.Total_uncompressed_size = pages.Sum(p => p.HeaderMeta.Uncompressed_page_size + p.HeaderSize);

         return chunk;
      }

      private List<PageTag> WriteValues(SchemaElement schema, IList values, Thrift.PageHeader ph, CompressionMethod compression)
      {
         var result = new List<PageTag>();
         byte[] dictionaryPageBytes = null;
         int dictionaryPageCount = 0;
         byte[] dataPageBytes;
         List<int> repetitions = null;
         List<int> definitions = null;

         //flatten values and create repetitions list if the field is repeatable
         if (schema.MaxRepetitionLevel > 0)
         {
            var rpack = new RepetitionPack();
            values = rpack.Unpack(_schema, values, out repetitions);
            ph.Data_page_header.Num_values = values.Count;
         }

         if (schema.IsNullable || schema.MaxDefinitionLevel > 0)
         {
            var dpack = new DefinitionPack();
            values = dpack.Unpack(values, _schema, out definitions);
         }

         using (var ms = new MemoryStream())
         {
            using (var writer = new BinaryWriter(ms))
            {
               //write repetitions
               if (repetitions != null)
               {
                  int bitWidth = PEncoding.GetWidthFromMaxInt(_schema.MaxRepetitionLevel);
                  RunLengthBitPackingHybridValuesWriter.Write(writer, bitWidth, repetitions);
               }

               //write definitions
               if (definitions != null)
               {
                  int bitWidth = PEncoding.GetWidthFromMaxInt(_schema.MaxDefinitionLevel);
                  RunLengthBitPackingHybridValuesWriter.Write(writer, bitWidth, definitions);
               }

               //write data
               if (!_writerOptions.UseDictionaryEncoding || !_dicWriter.Write(writer, schema, values, out IList dicValues))
               {
                  _plainWriter.Write(writer, schema, values, out IList plainExtra);
               }
               else
               {
                  dictionaryPageCount = dicValues.Count;
                  ph.Data_page_header.Encoding = Thrift.Encoding.PLAIN_DICTIONARY;
                  using (var dms = new MemoryStream())
                  using (var dwriter = new BinaryWriter(dms))
                  {
                     _plainWriter.Write(dwriter, schema, dicValues, out IList t0);
                     dictionaryPageBytes = dms.ToArray();
                  }
               }

               dataPageBytes = ms.ToArray();
            }
         }

         if (dictionaryPageBytes != null)
         {
            Thrift.PageHeader dph = _meta.CreateDictionaryPage(dictionaryPageCount);
            dictionaryPageBytes = Compress(dph, dictionaryPageBytes, compression);
            int dictionaryHeaderSize = Write(dph, dictionaryPageBytes);
            result.Add(new PageTag { HeaderSize = dictionaryHeaderSize, HeaderMeta = dph });
         }

         dataPageBytes = Compress(ph, dataPageBytes, compression);
         int dataHeaderSize = Write(ph, dataPageBytes);
         result.Add(new PageTag { HeaderSize = dataHeaderSize, HeaderMeta = ph });

         return result;
      }

      private int Write(Thrift.PageHeader ph, byte[] data)
      {
         int headerSize = _thriftStream.Write(ph);
         _output.Write(data, 0, data.Length);
         return headerSize;
      }

      private byte[] Compress(Thrift.PageHeader ph, byte[] data, CompressionMethod compression)
      {
         //note that page size numbers do not include header size by spec

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

   }
}
