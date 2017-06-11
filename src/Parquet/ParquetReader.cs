using Parquet.File;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Parquet
{
   public class ParquetReader : IDisposable
   {
      private readonly Stream _input;
      private readonly BinaryReader _reader;
      private readonly FileMetaData _meta;

      public ParquetReader(Stream input)
      {
         _input = input;
         _reader = new BinaryReader(input);

         _meta = ReadMetadata();
      }

      public void TestRead()
      {
         ColumnChunk cc = _meta.Row_groups[0].Columns[1];   //bool col

         var p = new Page(cc, _input);
      }

      private FileMetaData ReadMetadata()
      {
         //go to -4 bytes (PAR1) -4 bytes (footer length number)
         _input.Seek(-8, SeekOrigin.End);
         int footerLength = _reader.ReadInt32();
         char[] magic = _reader.ReadChars(4);

         //go to footer data and deserialize it
         _input.Seek(-8 - footerLength, SeekOrigin.End);
         return _input.ThriftRead<FileMetaData>();
      }

      public void Dispose()
      {
      }
   }
}