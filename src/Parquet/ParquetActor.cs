using System;
using System.IO;
using System.Text;
using Parquet.File;

namespace Parquet
{
   /// <summary>
   /// Base class for reader and writer
   /// </summary>
   public class ParquetActor
   {
#pragma warning disable IDE1006
      internal const string MagicString = "PAR1";
      internal static readonly byte[] MagicBytes = Encoding.ASCII.GetBytes(MagicString);
#pragma warning restore IDE1006

      private readonly Stream _fileStream;
      private BinaryReader _binaryReader;
      private BinaryWriter _binaryWriter;
      private ThriftStream _thriftStream;

      internal ParquetActor(Stream fileStream)
      {
         _fileStream = fileStream ?? throw new ArgumentNullException(nameof(fileStream));
      }

      /// <summary>
      /// Original stream to write or read
      /// </summary>
      protected Stream Stream => _fileStream;

      internal BinaryReader Reader => _binaryReader ?? (_binaryReader = new BinaryReader(_fileStream));

      internal BinaryWriter Writer => _binaryWriter ?? (_binaryWriter = new BinaryWriter(_fileStream));

      internal ThriftStream ThriftStream => _thriftStream ?? (_thriftStream = new ThriftStream(_fileStream));

      internal void ValidateFile()
      {
         _fileStream.Seek(0, SeekOrigin.Begin);
         char[] head = Reader.ReadChars(4);
         string shead = new string(head);
         _fileStream.Seek(-4, SeekOrigin.End);
         char[] tail = Reader.ReadChars(4);
         string stail = new string(tail);
         if (shead != MagicString)
            throw new IOException($"not a Parquet file(head is '{shead}')");
         if (stail != MagicString)
            throw new IOException($"not a Parquet file(head is '{stail}')");
      }

      internal Thrift.FileMetaData ReadMetadata()
      {
         GoBeforeFooter();

         return ThriftStream.Read<Thrift.FileMetaData>();
      }

      internal void GoToBeginning()
      {
         _fileStream.Seek(0, SeekOrigin.Begin);
      }

      internal void GoToEnd()
      {
         _fileStream.Seek(0, SeekOrigin.End);
      }

      internal void GoBeforeFooter()
      {
         //go to -4 bytes (PAR1) -4 bytes (footer length number)
         _fileStream.Seek(-8, SeekOrigin.End);
         int footerLength = Reader.ReadInt32();

         //set just before footer starts
         _fileStream.Seek(-8 - footerLength, SeekOrigin.End);
      }
   }
}
