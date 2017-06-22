using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Reflection;
using System.Text;
using Parquet.Thrift;
using Encoding = Parquet.Thrift.Encoding;
using Type = System.Type;
using TType = Parquet.Thrift.Type;
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

      private static readonly IValuesReader _plainReader = new PlainValuesReader();
      private static readonly IValuesReader _rleReader = new RunLengthBitPackingHybridValuesReader();
      private static readonly IValuesReader _dictionaryReader = new PlainDictionaryValuesReader();

      public PColumn(ColumnChunk thriftChunk, Schema schema, Stream inputStream, ThriftStream thriftStream)
      {
         if (thriftChunk.Meta_data.Path_in_schema.Count != 1)
            throw new NotImplementedException("path in scheme is not flat");

         _thriftChunk = thriftChunk;
         _thrift = thriftStream;
         _schema = schema;
         _inputStream = inputStream;
         _schemaElement = _schema[_thriftChunk];
      }

      public ParquetColumn Read()
      {
         string columnName = string.Join(".", _thriftChunk.Meta_data.Path_in_schema);
         var result = new ParquetColumn(columnName, _schemaElement);

         //get the minimum offset, we'll just read pages in sequence
         long offset = new[] { _thriftChunk.Meta_data.Dictionary_page_offset, _thriftChunk.Meta_data.Data_page_offset }.Where(e => e != 0).Min();
         long maxValues = _thriftChunk.Meta_data.Num_values;

         _inputStream.Seek(offset, SeekOrigin.Begin);

         PageHeader ph = _thrift.Read<PageHeader>();

         bool finished = false;
         IList dictionaryPage = null, copyPage = null;
         List<int> indexes = null;
         List<int> definitions = null;

         while (!finished)
         {
            if (ph.Type == PageType.DICTIONARY_PAGE)
            {
               (IList dictionaryPagePage, IList copyPagePage) = ReadDictionaryPage(ph);
               if(dictionaryPage == null)
               {
                  dictionaryPage = dictionaryPagePage;
               }
               else
               {
                  foreach(var item in dictionaryPagePage)
                  {
                     dictionaryPage.Add(item);
                  }
               }
               copyPage = copyPagePage;

               ph = _thrift.Read<PageHeader>(); //get next page
            }

            int dataPageCount = 0;
            while (true)
            {
               var page = ReadDataPage(ph, result.ValuesFinal);

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

               //if (page.definitions != null) throw new NotImplementedException();
               //if (page.repetitions != null) throw new NotImplementedException();

               //todo: combine tuple into real values

               if (result.ValuesFinal.Count >= maxValues || (indexes != null && indexes.Count >= maxValues))
               {
                  //all data pages read
                  finished = true;
                  break;
               }

               ph = _thrift.Read<PageHeader>(); //get next page
               if (ph.Type != PageType.DATA_PAGE)
               {
                  break;
               }
            }
         }

         // add the definitions into the result and the merge afterwards
         List<int> indexesMod = new List<int>();
         indexesMod = indexes != null
            ? indexes.Take(ph.Data_page_header.Num_values).ToList()
            : result.ValuesFinal.Cast<bool>().Select(item => item ? 1 : 0).ToList();


         var definitionsMod = definitions != null
            ? definitions.Take(ph.Data_page_header.Num_values).ToList()
            : new List<int>(Enumerable.Repeat(1, ph.Data_page_header.Num_values));

         if (copyPage == null) copyPage = result.ValuesFinal;

         var valuesList = new ParquetValueStructure(dictionaryPage, copyPage, indexesMod, definitionsMod);

         result.Add(valuesList);

         return result;
      }

      private static IList MergeDictionaryEncoding(IList dictionary, IList values)
      {
         //values will be ints if dictionary encoding is present
         int[] indexes = new int[values.Count];
         int i = 0;
         foreach(var value in values)
         {
            indexes[i++] = (int)value;
         }

         return indexes
            .Select(index => dictionary[index])
            .ToList();
      }

      private (IList, IList) ReadDictionaryPage(PageHeader ph)
      {
         //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain enncoding.

         byte[] data = ReadRawBytes(ph, _inputStream);

         using (var dataStream = new MemoryStream(data))
         {
            using (var dataReader = new BinaryReader(dataStream))
            {
               (IList result, IList copy) = ParquetColumn.CreateValuesList(_schemaElement, out Type systemType);
               _plainReader.Read(dataReader, _schemaElement, result);
               return (result, copy);
            }
         }
      }

      private (ICollection definitions, ICollection repetitions, List<int> indexes) ReadDataPage(PageHeader ph, IList destination)
      {
         byte[] data = ReadRawBytes(ph, _inputStream);

         using (var dataStream = new MemoryStream(data))
         {
            using (var reader = new BinaryReader(dataStream))
            {
               //todo: read repetition levels (only relevant for nested columns)

               List<int> definitions = ReadDefinitionLevels(reader, ph.Data_page_header.Num_values);

               // these are pointers back to the Values table - lookup on values 
               List<int> indexes = ReadColumnValues(reader, ph.Data_page_header.Encoding, destination);

               return (definitions, null, indexes);
            }
         }
      }

      private List<int> ReadDefinitionLevels(BinaryReader reader, int valueCount)
      {
         const int maxDefinitionLevel = 1;   //todo: for nested columns this needs to be calculated properly
         int bitWidth = PEncoding.GetWidthFromMaxInt(maxDefinitionLevel);
         var result = new List<int>();
         RunLengthBitPackingHybridValuesReader.ReadRleBitpackedHybrid(reader, bitWidth, 0, result);  //todo: there might be more data on larger files

         int maxLevel = _schema.GetMaxDefinitionLevel(_thriftChunk);
         int nullCount = valueCount - result.Count(r => r == maxLevel);
         if (nullCount == 0) return null;

         return result;
      }

      private List<int> ReadColumnValues(BinaryReader reader, Encoding encoding, IList destination)
      {
         //dictionary encoding uses RLE to encode data

         switch(encoding)
         {
            case Encoding.PLAIN:
               _plainReader.Read(reader, _schemaElement, destination);
               return null;

            case Encoding.RLE:
               var rleIndexes = new List<int>();
               _rleReader.Read(reader, _schemaElement, rleIndexes);
               return rleIndexes;

            case Encoding.PLAIN_DICTIONARY:
               var dicIndexes = new List<int>();
               _dictionaryReader.Read(reader, _schemaElement, dicIndexes);
               return dicIndexes;

            default:
               throw new Exception($"encoding {encoding} is not supported.");  //todo: replace with own exception type
         }
      }

      private static byte[] ReadRawBytes(PageHeader ph, Stream inputStream)
      {
         if (ph.Compressed_page_size != ph.Uncompressed_page_size)
            throw new NotImplementedException("compressed pages not supported");

         byte[] data = new byte[ph.Compressed_page_size];
         inputStream.Read(data, 0, data.Length);

         //todo: uncompress page

         return data;
      }
   }
}
