using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Text;
using Parquet.Thrift;
using Encoding = Parquet.Thrift.Encoding;
using Type = Parquet.Thrift.Type;

namespace Parquet.File
{
   class PColumn
   {
      private readonly ColumnChunk _thriftChunk;
      private readonly Stream _inputStream;
      private readonly ThriftStream _thrift;
      private readonly Schema _schema;
      private readonly SchemaElement _schemaElement;

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
         IList result = null;

         //get the minimum offset, we'll just read pages in sequence
         long offset = new[] { _thriftChunk.Meta_data.Dictionary_page_offset, _thriftChunk.Meta_data.Data_page_offset }.Where(e => e != 0).Min();

         _inputStream.Seek(offset, SeekOrigin.Begin);

         PageHeader ph = _thrift.Read<PageHeader>();

         IList dictionaryPage = null;
         if (ph.Type == PageType.DICTIONARY_PAGE)
         {
            dictionaryPage = ReadDictionaryPage(ph);

            ph = _thrift.Read<PageHeader>(); //get next page
         }

         long num = 0;
         while(true)
         {
            var page = ReadDataPage(ph);

            if (page.definitions != null) throw new NotImplementedException();
            if (page.repetitions != null) throw new NotImplementedException();

            //todo: combine tuple into real values

            //add values
            if(result == null)
            {
               result = page.values;
            }
            else
            {
               foreach(var value in page.values)
               {
                  result.Add(value);
               }
            }

            num += page.values.Count;

            if (num >= _thriftChunk.Meta_data.Num_values)
            {
               //all data pages read
               break;
            }
            ph = _thrift.Read<PageHeader>(); //get next page
         }

         return new ParquetColumn(
            string.Join(".", _thriftChunk.Meta_data.Path_in_schema),
            dictionaryPage == null
               ? result
               : MergeDictionaryEncoding(dictionaryPage, result));
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

      private IList ReadDictionaryPage(PageHeader ph)
      {
         //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain enncoding.

         byte[] data = ReadRawBytes(ph, _inputStream);

         using (var dataStream = new MemoryStream(data))
         {
            using (var dataReader = new BinaryReader(dataStream))
            {
               return PEncoding.ReadPlain(dataReader, _schemaElement);
            }
         }
      }

      private (ICollection definitions, ICollection repetitions, IList values) ReadDataPage(PageHeader ph)
      {
         byte[] data = ReadRawBytes(ph, _inputStream);

         using (var dataStream = new MemoryStream(data))
         {
            using (var reader = new BinaryReader(dataStream))
            {
               //todo: read repetition levels (only relevant for nested columns)

               List<int> definitions = ReadDefinitionLevels(reader, ph.Data_page_header.Num_values);

               IList values = ReadColumnValues(reader, ph.Data_page_header.Encoding);

               return (definitions, null, values);
            }
         }
      }

      private List<int> ReadDefinitionLevels(BinaryReader reader, int valueCount)
      {
         const int maxDefinitionLevel = 1;   //todo: for nested columns this needs to be calculated properly
         int bitWidth = PEncoding.GetWidthFromMaxInt(maxDefinitionLevel);
         List<int> result = PEncoding.ReadRleBitpackedHybrid(reader, bitWidth, 0);  //todo: there might be more data on larger files

         int maxLevel = _schema.GetMaxDefinitionLevel(_thriftChunk);
         int nullCount = valueCount - result.Count(r => r == maxLevel);
         if (nullCount == 0) return null;

         return result;
      }

      private IList ReadColumnValues(BinaryReader reader, Encoding encoding)
      {
         //dictionary encoding uses RLE to encode data

         switch(encoding)
         {
            case Encoding.PLAIN:
               return PEncoding.ReadPlain(reader, _schemaElement);   //todo: it's not just boolean! use Richard's implementation

            case Encoding.RLE:
            case Encoding.PLAIN_DICTIONARY:

               int bitWidth;
               if (encoding == Encoding.RLE)
                  bitWidth = _schemaElement.Type_length;
               else
                  bitWidth = reader.ReadByte();

               int length = GetRemainingLength(reader);
               return PEncoding.ReadRleBitpackedHybrid(reader, bitWidth, length);

            default:
               throw new Exception($"encoding {encoding} is not supported.");  //todo: replace with own exception type
         }
      }

      private int GetRemainingLength(BinaryReader reader)
      {
         return (int)(reader.BaseStream.Length - reader.BaseStream.Position);
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
