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
      private readonly ThriftFooter _footer;
      private readonly Thrift.SchemaElement _tse;
      private readonly CompressionMethod _compressionMethod;
      private readonly ParquetOptions _formatOptions;
      private readonly WriterOptions _writerOptions;
      private readonly IDataTypeHandler _dataTypeHandler;
      private readonly Thrift.ColumnChunk _chunk;
      private readonly Thrift.PageHeader _ph;
      private readonly int _maxRepetitionLevel;
      private readonly int _maxDefinitionLevel;

      private struct PageTag
      {
         public int HeaderSize;
         public Thrift.PageHeader HeaderMeta;
      }

      public ColumnWriter(Stream output, ThriftStream thriftStream,
         ThriftFooter footer,
         Thrift.SchemaElement tse, List<string> path,
         CompressionMethod compressionMethod,
         ParquetOptions formatOptions,
         WriterOptions writerOptions)
      {
         _output = output;
         _thriftStream = thriftStream;
         _footer = footer;
         _tse = tse;
         _compressionMethod = compressionMethod;
         _formatOptions = formatOptions;
         _writerOptions = writerOptions;
         _dataTypeHandler = DataTypeFactory.Match(tse, _formatOptions);

         _chunk = _footer.CreateColumnChunk(_compressionMethod, _output, _tse.Type, path, 0);
         _ph = _footer.CreateDataPage(0);
         _footer.GetLevels(_chunk, out int maxRepetitionLevel, out int maxDefinitionLevel);
         _maxRepetitionLevel = maxRepetitionLevel;
         _maxDefinitionLevel = maxDefinitionLevel;
      }

      public Thrift.ColumnChunk Write(int offset, int count, IList values)
      {
         _ph.Data_page_header.Num_values = values.Count;
         List<PageTag> pages = WriteValues(values);
         _chunk.Meta_data.Num_values = _ph.Data_page_header.Num_values;

         //the following counters must include both data size and header size
         _chunk.Meta_data.Total_compressed_size = pages.Sum(p => p.HeaderMeta.Compressed_page_size + p.HeaderSize);
         _chunk.Meta_data.Total_uncompressed_size = pages.Sum(p => p.HeaderMeta.Uncompressed_page_size + p.HeaderSize);

         return _chunk;
      }

      private List<PageTag> WriteValues(IList values)
      {
         var result = new List<PageTag>();
         byte[] dataPageBytes;
         List<int> repetitions = null;
         List<int> definitions = null;

         //flatten values and create repetitions list if the field is repeatable
         if (_maxRepetitionLevel > 0)
         {
            repetitions = new List<int>();
            IList flatValues = _dataTypeHandler.CreateEmptyList(_tse.IsNullable(), 0);
            RepetitionPack.HierarchyToFlat(_maxRepetitionLevel, values, flatValues, repetitions);
            values = flatValues;
            _ph.Data_page_header.Num_values = values.Count; //update with new count
         }

         if (_maxDefinitionLevel > 0)
         {
            definitions = DefinitionPack.RemoveNulls(values, _maxDefinitionLevel);
         }

         using (var ms = new MemoryStream())
         {
            using (var writer = new BinaryWriter(ms))
            {
               //write repetitions
               if (repetitions != null)
               {
                  WriteLevels(writer, repetitions, _maxRepetitionLevel);
               }

               //write definitions
               if (definitions != null)
               {
                  WriteLevels(writer, definitions, _maxDefinitionLevel);
               }

               //write data
               _dataTypeHandler.Write(_tse, writer, values);

               dataPageBytes = ms.ToArray();
            }
         }

         dataPageBytes = Compress(dataPageBytes);
         int dataHeaderSize = Write(dataPageBytes);
         result.Add(new PageTag { HeaderSize = dataHeaderSize, HeaderMeta = _ph });

         return result;
      }

      private void WriteLevels(BinaryWriter writer, List<int> levels, int maxLevel)
      {
         int bitWidth = PEncoding.GetWidthFromMaxInt(maxLevel);
         RunLengthBitPackingHybridValuesWriter.Write(writer, bitWidth, levels);
      }

      private int Write(byte[] data)
      {
         int headerSize = _thriftStream.Write(_ph);
         _output.Write(data, 0, data.Length);
         return headerSize;
      }

      private byte[] Compress(byte[] data)
      {
         //note that page size numbers do not include header size by spec

         _ph.Uncompressed_page_size = data.Length;
         byte[] result;

         if (_compressionMethod != CompressionMethod.None)
         {
            IDataWriter writer = DataFactory.GetWriter(_compressionMethod);
            using (var ms = new MemoryStream())
            {
               writer.Write(data, ms);
               result = ms.ToArray();
            }
            _ph.Compressed_page_size = result.Length;
         }
         else
         {
            _ph.Compressed_page_size = _ph.Uncompressed_page_size;
            result = data;
         }

         return result;
      }

   }
}
