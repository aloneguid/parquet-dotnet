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
   class Page
   {
      private readonly ColumnChunk _thriftChunk;
      private readonly Stream _inputStream;
      private readonly Schema _schema;

      public Page(ColumnChunk thriftChunk, Schema schema, Stream inputStream)
      {
         if (thriftChunk.Meta_data.Path_in_schema.Count != 1)
            throw new NotImplementedException("path in scheme is not flat");

         _thriftChunk = thriftChunk;
         _schema = schema;
         _inputStream = inputStream;

         Read();
      }

      private void Read()
      {
         //get the minimum offset, we'll just read pages in sequence
         long offset = new[] { _thriftChunk.Meta_data.Dictionary_page_offset, _thriftChunk.Meta_data.Data_page_offset }.Where(e => e != 0).Min();

         _inputStream.Seek(offset, SeekOrigin.Begin);

         PageHeader ph = _inputStream.ThriftRead<PageHeader>();

         if(ph.Type == PageType.DICTIONARY_PAGE)
         {
            ICollection dictionaryPage = ReadDictionaryPage(ph);

            ph = _inputStream.ThriftRead<PageHeader>(); //get next page
         }

         long num = 0;
         while(true)
         {
            var page = ReadPage(ph);

            if (num >= _thriftChunk.Meta_data.Num_values)
            {
               //all data pages read
               break;
            }
            ph = _inputStream.ThriftRead<PageHeader>(); //get next page
         }
      }

      private ICollection ReadDictionaryPage(PageHeader ph)
      {
         //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain enncoding.

         byte[] data = ReadRawBytes(ph, _inputStream);

         using (var dataStream = new MemoryStream(data))
         {
            using (var dataReader = new BinaryReader(dataStream))
            {
               return PEncoding.ReadPlain(dataReader, _thriftChunk.Meta_data.Type);
            }
         }
      }

      private (ICollection definitions, ICollection repetitions, ICollection values) ReadPage(PageHeader ph)
      {
         byte[] data = ReadRawBytes(ph, _inputStream);

         using (var dataStream = new MemoryStream(data))
         {
            using (var dataReader = new BinaryReader(dataStream))
            {
               //todo: read repetition levels (only relevant for nested columns)

               //read definition levels
               ICollection definitions = ReadData(dataReader, ph.Data_page_header.Definition_level_encoding);

               //read actual data
               ICollection values = ReadData(dataReader, ph.Data_page_header.Encoding);

               return (definitions, null, values);
            }
         }
      }

      private ICollection ReadData(BinaryReader reader, Encoding encoding)
      {
         switch(encoding)
         {
            case Encoding.RLE:               //this is RLE/Bitpacking hybrid, not just RLE
            case Encoding.PLAIN_DICTIONARY:  //dictinary page is also encoded in RLE, but needs merging upwards
               return PEncoding.ReadRleBitpackedHybrid(reader);
            case Encoding.PLAIN:
               return PEncoding.ReadPlain(reader, Type.BOOLEAN);   //todo: it's not just boolean!
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
