using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Parquet.Data;
using Parquet.File.Data;
using Parquet.File.Values;

namespace Parquet.File
{
   class ColumnarReader
   {
      private readonly Stream _inputStream;
      private readonly Thrift.ColumnChunk _thriftColumnChunk;
      private readonly Thrift.SchemaElement _thriftSchemaElement;
      private readonly ThriftFooter _footer;
      private readonly ParquetOptions _parquetOptions;
      private readonly ThriftStream _thriftStream;
      private readonly int _maxRepetitionLevel;
      private readonly int _maxDefinitionLevel;
      private readonly IDataTypeHandler _dataTypeHandler;

      private class PageData
      {
         public List<int> definitions;
         public List<int> repetitions;
         public List<int> indexes;
         public IList values;
      }

      public ColumnarReader(Stream inputStream, Thrift.ColumnChunk thriftColumnChunk, ThriftFooter footer, ParquetOptions parquetOptions)
      {
         _inputStream = inputStream ?? throw new ArgumentNullException(nameof(inputStream));
         _thriftColumnChunk = thriftColumnChunk ?? throw new ArgumentNullException(nameof(thriftColumnChunk));
         _footer = footer ?? throw new ArgumentNullException(nameof(footer));
         _parquetOptions = parquetOptions ?? throw new ArgumentNullException(nameof(parquetOptions));

         _thriftStream = new ThriftStream(inputStream);
         _footer.GetLevels(_thriftColumnChunk, out int mrl, out int mdl);
         _maxRepetitionLevel = mrl;
         _maxDefinitionLevel = mdl;
         _thriftSchemaElement = _footer.GetSchemaElement(_thriftColumnChunk);
         _dataTypeHandler = DataTypeFactory.Match(_thriftSchemaElement, _parquetOptions);
      }

      public IList Read(long offset, long count)
      {
         long fileOffset = GetFileOffset();
         long maxValues = _thriftColumnChunk.Meta_data.Num_values;

         _inputStream.Seek(fileOffset, SeekOrigin.Begin);

         IList dictionary = null;
         List<int> indexes = null;
         List<int> repetitions = null;
         List<int> definitions = null;
         IList values = null;

         //there can be only one dictionary page in column
         Thrift.PageHeader ph = _thriftStream.Read<Thrift.PageHeader>();
         if (TryReadDictionaryPage(ph, out dictionary)) ph = _thriftStream.Read<Thrift.PageHeader>();

         int pagesRead = 0;
         while (true)
         {
            int valuesSoFar = Math.Max(indexes == null ? 0 : indexes.Count, values == null ? 0 : values.Count);
            PageData pd = ReadDataPage(ph, maxValues - valuesSoFar);

            repetitions = AssignOrAdd(repetitions, pd.repetitions);
            definitions = AssignOrAdd(definitions, pd.definitions);
            indexes = AssignOrAdd(indexes, pd.indexes);
            values = AssignOrAdd(values, pd.values);

            pagesRead++;

            int totalCount = Math.Max(
               (values == null ? 0 : values.Count) +
               (indexes == null ? 0 : indexes.Count),
               (definitions == null ? 0 : definitions.Count));
            if (totalCount >= maxValues) break; //limit reached

            ph = _thriftStream.Read<Thrift.PageHeader>();
            if (ph.Type != Thrift.PageType.DATA_PAGE) break;
         }

         IList mergedValues = new ValueMerger(
            _maxRepetitionLevel,
            () => _dataTypeHandler.CreateEmptyList(_thriftSchemaElement.IsNullable(), 0),
            values ?? _dataTypeHandler.CreateEmptyList(_thriftSchemaElement.IsNullable(), 0))
            .Apply(dictionary, definitions, repetitions, indexes, (int)maxValues);

         ValueMerger.Trim(mergedValues, (int)offset, (int)count);

         return mergedValues;
      }

      private bool TryReadDictionaryPage(Thrift.PageHeader ph, out IList dictionary)
      {
         if (ph.Type != Thrift.PageType.DICTIONARY_PAGE)
         {
            dictionary = null;
            return false;
         }

         //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain encoding.

         byte[] data = ReadRawBytes(ph, _inputStream);

         using (var dataStream = new MemoryStream(data))
         {
            using (var dataReader = new BinaryReader(dataStream))
            {
               dictionary = _dataTypeHandler.Read(_thriftSchemaElement, dataReader, _parquetOptions);
               return true;
            }
         }
      }

      private PageData ReadDataPage(Thrift.PageHeader ph, long maxValues)
      {
         byte[] data = ReadRawBytes(ph, _inputStream);
         int max = ph.Data_page_header.Num_values;

         var pd = new PageData();

         using (var dataStream = new MemoryStream(data))
         {
            using (var reader = new BinaryReader(dataStream))
            {
               if(_maxRepetitionLevel > 0)
               {
                  pd.repetitions = ReadLevels(reader, _maxRepetitionLevel);
               }

               if(_maxDefinitionLevel > 0)
               {
                  pd.definitions = ReadLevels(reader, _maxDefinitionLevel);
               }

               ReadColumn(reader, ph.Data_page_header.Encoding, maxValues,
                  out pd.values,
                  out pd.indexes);
            }
         }

         return pd;
      }

      private void ReadColumn(BinaryReader reader, Thrift.Encoding encoding, long maxValues,
         out IList values,
         out List<int> indexes)
      {
         //dictionary encoding uses RLE to encode data

         switch (encoding)
         {
            case Thrift.Encoding.PLAIN:
               values = _dataTypeHandler.Read(_thriftSchemaElement, reader, _parquetOptions);
               indexes = null;
               break;

            case Thrift.Encoding.RLE:
               values = null;
               indexes = RunLengthBitPackingHybridValuesReader.Read(reader, _thriftSchemaElement.Type_length);
               break;

            case Thrift.Encoding.PLAIN_DICTIONARY:
               values = null;
               indexes = ReadPlainDictionary(reader, maxValues);
               break;

            default:
               throw new ParquetException($"encoding {encoding} is not supported.");
         }
      }

      private static List<int> ReadPlainDictionary(BinaryReader reader, long maxValues)
      {
         var result = new List<int>();
         int bitWidth = reader.ReadByte();

         //when bit width is zero reader must stop and just repeat zero maxValue number of times
         if (bitWidth == 0)
         {
            for (int i = 0; i < maxValues; i++)
            {
               result.Add(0);
            }
            return result;
         }

         int length = GetRemainingLength(reader);
         RunLengthBitPackingHybridValuesReader.ReadRleBitpackedHybrid(reader, bitWidth, length, result);
         return result;
      }

      /// <summary>
      /// Reads levels, suitable for both repetition levels and definition levels
      /// </summary>
      /// <param name="reader"></param>
      /// <param name="maxLevel">Maximum level value, depends on level type</param>
      /// <returns></returns>
      private List<int> ReadLevels(BinaryReader reader, int maxLevel)
      {
         int bitWidth = PEncoding.GetWidthFromMaxInt(maxLevel);
         var result = new List<int>();

         //todo: there might be more data on larger files, therefore line below need to be called in a loop until valueCount is satisfied
         RunLengthBitPackingHybridValuesReader.ReadRleBitpackedHybrid(reader, bitWidth, 0, result);

         return result;
      }


      private byte[] ReadRawBytes(Thrift.PageHeader ph, Stream inputStream)
      {
         Thrift.CompressionCodec thriftCodec = _thriftColumnChunk.Meta_data.Codec;
         IDataReader reader = DataFactory.GetReader(thriftCodec);

         return reader.Read(inputStream, ph.Compressed_page_size);
      }

      private long GetFileOffset()
      {
         //get the minimum offset, we'll just read pages in sequence

         return
            new[]
            {
               _thriftColumnChunk.Meta_data.Dictionary_page_offset,
               _thriftColumnChunk.Meta_data.Data_page_offset
            }
            .Where(e => e != 0)
            .Min();
      }

      private List<int> AssignOrAdd(List<int> container, List<int> source)
      {
         if (source != null)
         {
            if (container == null)
            {
               container = source;
            }
            else
            {
               container.AddRange(source);
            }
         }

         return container;
      }

      private IList AssignOrAdd(IList container, IList source)
      {
         if (source != null)
         {
            if (container == null)
            {
               container = source;
            }
            else
            {
               foreach(object item in source)
               {
                  container.Add(item);
               }
            }
         }

         return container;
      }

      private static int GetRemainingLength(BinaryReader reader)
      {
         return (int)(reader.BaseStream.Length - reader.BaseStream.Position);
      }
   }
}