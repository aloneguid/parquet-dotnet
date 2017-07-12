using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.File.Values;
using Parquet.Data;
using Parquet.File.Data;

namespace Parquet.File
{
   class PColumn
   {
      private readonly Thrift.ColumnChunk _thriftChunk;
      private readonly Stream _inputStream;
      private readonly ThriftStream _thrift;
      private readonly Schema _schema;
      private readonly SchemaElement _schemaElement;
      private readonly ParquetOptions _options;

      private readonly IValuesReader _plainReader;
      private static readonly IValuesReader _rleReader = new RunLengthBitPackingHybridValuesReader();
      private static readonly IValuesReader _dictionaryReader = new PlainDictionaryValuesReader();

      public PColumn(Thrift.ColumnChunk thriftChunk, Schema schema, Stream inputStream, ThriftStream thriftStream, ParquetOptions options)
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

      public IList Read(string columnName)
      {
         IList values = TypeFactory.Create(_schemaElement);

         //get the minimum offset, we'll just read pages in sequence
         long offset = new[] { _thriftChunk.Meta_data.Dictionary_page_offset, _thriftChunk.Meta_data.Data_page_offset }.Where(e => e != 0).Min();
         long maxValues = _thriftChunk.Meta_data.Num_values;

         _inputStream.Seek(offset, SeekOrigin.Begin);

         Thrift.PageHeader ph = _thrift.Read<Thrift.PageHeader>();

         IList dictionaryPage = null;
         List<int> indexes = null;
         List<int> definitions = null;

         //there can be only one dictionary page in column
         if (ph.Type == Thrift.PageType.DICTIONARY_PAGE)
         {
            dictionaryPage = ReadDictionaryPage(ph);
            ph = _thrift.Read<Thrift.PageHeader>(); //get next page after dictionary
         }

         int dataPageCount = 0;
         while (true)
         {
            int valuesSoFar = Math.Max(indexes == null ? 0 : indexes.Count, values.Count);
            var page = ReadDataPage(ph, values, maxValues - valuesSoFar);

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

            if((values.Count >= maxValues) || (indexes != null && indexes.Count >= maxValues) || (definitions != null && definitions.Count >= maxValues))
            {
               break;   //limit reached
            }

            /*IList acc1 = new ValueMerger(_schemaElement, values).Apply(dictionaryPage, definitions, indexes, maxValues);
            dictionaryPage = null;
            definitions = null;
            indexes = null;
            values.Clear();
            foreach (var el in acc1) acc.Add(el);*/

            ph = _thrift.Read<Thrift.PageHeader>(); //get next page
            if (ph.Type != Thrift.PageType.DATA_PAGE)
            {
               break;
            }
         }

         IList mergedValues = new ValueMerger(_schemaElement, values).Apply(dictionaryPage, definitions, indexes, maxValues);

         return mergedValues;
      }

      private IList ReadDictionaryPage(Thrift.PageHeader ph)
      {
         //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain enncoding.

         byte[] data = ReadRawBytes(ph, _inputStream);

         using (var dataStream = new MemoryStream(data))
         {
            using (var dataReader = new BinaryReader(dataStream))
            {
               IList result = TypeFactory.Create(_schemaElement);
               _plainReader.Read(dataReader, _schemaElement, result, int.MaxValue);
               return result;
            }
         }
      }

      private (ICollection definitions, ICollection repetitions, List<int> indexes) ReadDataPage(Thrift.PageHeader ph, IList destination, long maxValues)
      {
         byte[] data = ReadRawBytes(ph, _inputStream);

         using (var dataStream = new MemoryStream(data))
         {
            using (var reader = new BinaryReader(dataStream))
            {
               //todo: read repetition levels (only relevant for nested columns)

               //check if there are definitions at all
               bool hasDefinitions = _schemaElement.HasDefinitionLevelsPage(ph);
               List<int> definitions = hasDefinitions
                  ? ReadDefinitionLevels(reader, (int)maxValues)
                  : null;

               // these are pointers back to the Values table - lookup on values 
               List<int> indexes = ReadColumnValues(reader, ph.Data_page_header.Encoding, destination, maxValues);

               //trim output if it exceeds max number of values
               int numValues = ph.Data_page_header.Num_values;
               if(definitions != null) ValueMerger.Trim(definitions, numValues);
               if(indexes != null) ValueMerger.Trim(indexes, numValues);

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
         ValueMerger.Trim(result, valueCount);  //trim result so null count procudes correct value
         int nullCount = valueCount - result.Count(r => r == maxLevel);
         if (nullCount == 0) return null;

         return result;
      }

      private List<int> ReadColumnValues(BinaryReader reader, Thrift.Encoding encoding, IList destination, long maxValues)
      {
         //dictionary encoding uses RLE to encode data

         switch(encoding)
         {
            case Thrift.Encoding.PLAIN:
               _plainReader.Read(reader, _schemaElement, destination, maxValues);
               return null;

            case Thrift.Encoding.RLE:
               var rleIndexes = new List<int>();
               _rleReader.Read(reader, _schemaElement, rleIndexes, maxValues);
               return rleIndexes;

            case Thrift.Encoding.PLAIN_DICTIONARY:
               var dicIndexes = new List<int>();
               _dictionaryReader.Read(reader, _schemaElement, dicIndexes, maxValues);
               return dicIndexes;

            default:
               throw new ParquetException($"encoding {encoding} is not supported.");  //todo: replace with own exception type
         }
      }

      private byte[] ReadRawBytes(Thrift.PageHeader ph, Stream inputStream)
      {
         Thrift.CompressionCodec thriftCodec = _thriftChunk.Meta_data.Codec;
         IDataReader reader = DataFactory.GetReader(thriftCodec);

         return reader.Read(inputStream, ph.Compressed_page_size);
      }
   }
}
