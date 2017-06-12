using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
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

         //read definition levels



         //bool[] values = new BitArray(data).ConvertToBoolArray(count);/


         //assume plain encoding



      }
    }
}
