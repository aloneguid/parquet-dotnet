using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Numerics;
using System.Text;

namespace Parquet.File
{
   class Page
   {
      private readonly ColumnChunk _thriftChunk;
      private readonly Stream _inputStream;
      private PageHeader _ph;

      public Page(ColumnChunk thriftChunk, Stream inputStream)
      {
         _thriftChunk = thriftChunk;
         _inputStream = inputStream;

         Read();
      }

      private void Read()
      {
         long offset = _thriftChunk.Meta_data.Data_page_offset;

         _inputStream.Seek(offset, SeekOrigin.Begin);

         //chunk:
         //encoding: RLE, PLAIN_DICTIONARY, PLAIN

         _ph = _inputStream.ThriftRead<PageHeader>();

         int count = _ph.Data_page_header.Num_values;

         byte[] data = new byte[_ph.Compressed_page_size];
         int read = _inputStream.Read(data, 0, data.Length);

         using (var dataStream = new MemoryStream(data))
         {
            using (var dataReader = new BinaryReader(dataStream))
            {
               //todo: read repetition levels

               //read definition levels
               ReadData(dataReader, _ph.Data_page_header.Definition_level_encoding);

               //read actual data
               ReadData(dataReader, _ph.Data_page_header.Encoding);
            }
         }
         //assume plain encoding
      }

      private void ReadData(BinaryReader reader, Encoding encoding)
      {
         switch(encoding)
         {
            case Encoding.RLE:   //this is RLE/Bitpacking hybrid, not just RLE
               PEncoding.ReadRleBitpackedHybrid(reader);
               break;
            case Encoding.PLAIN:
               PEncoding.ReadPlain(reader);
               break;
            default:
               throw new ApplicationException($"encoding {encoding} is not supported.");  //todo: replace with own exception type
         }
      }


    }
}
