using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.Thrift;
using Encoding = Parquet.Thrift.Encoding;
using Type = System.Type;
using Parquet.File.Values;

namespace Parquet.File
{
   class PColumn
   {
      private readonly ColumnChunk _thriftChunk;
      private readonly Stream _inputStream;
      private readonly ThriftStream _thrift;
      private readonly Schema _schema;
      private readonly SchemaElement _schemaElement;
      private readonly ParquetOptions _options;

      private readonly IValuesReader _plainReader;
      private static readonly IValuesReader _rleReader = new RunLengthBitPackingHybridValuesReader();
      private static readonly IValuesReader _dictionaryReader = new PlainDictionaryValuesReader();

      public PColumn(ColumnChunk thriftChunk, Schema schema, Stream inputStream, ThriftStream thriftStream, ParquetOptions options)
      {
         if (thriftChunk.Meta_data.Path_in_schema.Count != 1)
            throw new NotImplementedException("path in scheme is not flat");

         _thriftChunk = thriftChunk;
         _thrift = thriftStream;
         _schema = schema;
         _inputStream = inputStream;
         _schemaElement = _schema[_thriftChunk];
         _options = options;

         _plainReader = new PlainValuesReader(options);
      }

      public ParquetColumn Read(string columnName)
      {
         var result = new ParquetColumn(columnName, _schemaElement);

         //get the minimum offset, we'll just read pages in sequence
         long offset = new[] { _thriftChunk.Meta_data.Dictionary_page_offset, _thriftChunk.Meta_data.Data_page_offset }.Where(e => e != 0).Min();
         long maxValues = _thriftChunk.Meta_data.Num_values;

         _inputStream.Seek(offset, SeekOrigin.Begin);

         PageHeader ph = _thrift.Read<PageHeader>();

         IList dictionaryPage = null;
         List<int> indexes = null;
         List<int> definitions = null;

         //there can be only one dictionary page in column
         if (ph.Type == PageType.DICTIONARY_PAGE)
         {
            dictionaryPage = ReadDictionaryPage(ph);
            ph = _thrift.Read<PageHeader>(); //get next page after dictionary
         }

         int dataPageCount = 0;
         while (true)
         {
            int valuesSoFar = Math.Max(indexes == null ? 0 : indexes.Count, result.Values.Count);
            var page = ReadDataPage(ph, result.Values, maxValues - valuesSoFar);

            //merge indexes
            if (page.indexes != null)
            {
               if (indexes == null)
               {
                  indexes = page.indexes;
               }
               else
               {
                  indexes.AddRange(page.indexes);
               }
            }

            if (page.definitions != null)
            {
               if (definitions == null)
               {
                  definitions = (List<int>) page.definitions;
               }
               else
               {
                  definitions.AddRange((List<int>) page.definitions);
               }
            }

            dataPageCount++;

            if (page.repetitions != null) throw new NotImplementedException();

            if((result.Values.Count >= maxValues) || (indexes != null && indexes.Count >= maxValues) || (definitions != null && definitions.Count >= maxValues))
            {
               break;   //limit reached
            }

            ph = _thrift.Read<PageHeader>(); //get next page
            if (ph.Type != PageType.DATA_PAGE)
            {
               break;
            }
         }

         new ValueMerger(result).Apply(dictionaryPage, definitions, indexes, maxValues);

         return result;
      }

      private IList ReadDictionaryPage(PageHeader ph)
      {
         //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain enncoding.

         byte[] data = ReadRawBytes(ph, _inputStream);

         using (var dataStream = new MemoryStream(data))
         {
            using (var dataReader = new BinaryReader(dataStream))
            {
               IList result = ParquetColumn.CreateValuesList(_schemaElement, out Type systemType);
               _plainReader.Read(dataReader, _schemaElement, result, int.MaxValue);
               return result;
            }
         }
      }

      private (ICollection definitions, ICollection repetitions, List<int> indexes) ReadDataPage(PageHeader ph, IList destination, long maxValues)
      {
         byte[] data = ReadRawBytes(ph, _inputStream);

         using (var dataStream = new MemoryStream(data))
         {
            using (var reader = new BinaryReader(dataStream))
            {
               //todo: read repetition levels (only relevant for nested columns)

               List<int> definitions = ReadDefinitionLevels(reader, (int)maxValues);

               // these are pointers back to the Values table - lookup on values 
               List<int> indexes = ReadColumnValues(reader, ph.Data_page_header.Encoding, destination, maxValues);

               return (definitions, null, indexes);
            }
         }
      }

      private List<int> ReadDefinitionLevels(BinaryReader reader, int valueCount)
      {
         const int maxDefinitionLevel = 1;   //todo: for nested columns this needs to be calculated properly
         int bitWidth = PEncoding.GetWidthFromMaxInt(maxDefinitionLevel);
         var result = new List<int>();
         //todo: there might be more data on larger files, therefore line below need to be called in a loop until valueCount is satisfied
         RunLengthBitPackingHybridValuesReader.ReadRleBitpackedHybrid(reader, bitWidth, 0, result, valueCount);

         int maxLevel = _schema.GetMaxDefinitionLevel(_thriftChunk);
         int nullCount = valueCount - result.Count(r => r == maxLevel);
         if (nullCount == 0) return null;

         return result;
      }

      private List<int> ReadColumnValues(BinaryReader reader, Encoding encoding, IList destination, long maxValues)
      {
         //dictionary encoding uses RLE to encode data

         switch(encoding)
         {
            case Encoding.PLAIN:
               _plainReader.Read(reader, _schemaElement, destination, maxValues);
               return null;

            case Encoding.RLE:
               var rleIndexes = new List<int>();
               _rleReader.Read(reader, _schemaElement, rleIndexes, maxValues);
               return rleIndexes;

            case Encoding.PLAIN_DICTIONARY:
               var dicIndexes = new List<int>();
               _dictionaryReader.Read(reader, _schemaElement, dicIndexes, maxValues);
               return dicIndexes;

            default:
               throw new ParquetException($"encoding {encoding} is not supported.");  //todo: replace with own exception type
         }
      }

      private static byte[] ReadRawBytes(PageHeader ph, Stream inputStream)
      {
         if (ph.Compressed_page_size != ph.Uncompressed_page_size)
            throw new ParquetException("compressed pages not supported");

         byte[] data = new byte[ph.Compressed_page_size];
         inputStream.Read(data, 0, data.Length);

         //todo: uncompress page

         return data;
      }
   }
}
