using Parquet.File;
using System;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;
using System.Collections;
using Parquet.Data.Predicates;
using System.Linq;

namespace Parquet
{
   /// <summary>
   /// Implements Apache Parquet format reader, experimental version for next major release.
   /// </summary>
   internal class ParquetReader3 : ParquetActor, IDisposable
   {
      private readonly Stream _input;
      private readonly Thrift.FileMetaData _meta;
      private readonly ThriftFooter _footer;
      private readonly ParquetOptions _parquetOptions;
      private readonly ReaderOptions _readerOptions;
      private readonly List<ParquetRowGroupReader> _groupReaders = new List<ParquetRowGroupReader>();

      /// <summary>
      /// Creates an instance from input stream
      /// </summary>
      /// <param name="input">Input stream, must be readable and seekable</param>
      /// <param name="parquetOptions">Optional reader options</param>
      /// <param name="readerOptions">The reader options.</param>
      /// <exception cref="ArgumentNullException">input</exception>
      /// <exception cref="ArgumentException">stream must be readable and seekable - input</exception>
      /// <exception cref="IOException">not a Parquet file (size too small)</exception>
      public ParquetReader3(Stream input, ParquetOptions parquetOptions = null, ReaderOptions readerOptions = null) : base(input)
      {
         _input = input ?? throw new ArgumentNullException(nameof(input));
         if (!input.CanRead || !input.CanSeek) throw new ArgumentException("stream must be readable and seekable", nameof(input));
         if (_input.Length <= 8) throw new IOException("not a Parquet file (size too small)");

         ValidateFile();
         _parquetOptions = parquetOptions ?? new ParquetOptions();
         _readerOptions = readerOptions ?? new ReaderOptions();

         //read metadata instantly, now
         _meta = ReadMetadata();
         _footer = new ThriftFooter(_meta);

         InitRowGroupReaders();
      }

      /// <summary>
      /// Gets the number of rows groups in this file
      /// </summary>
      public int RowGroupCount => _meta.Row_groups.Count;

      public ParquetRowGroupReader OpenRowGroupReader(int index)
      {
         return _groupReaders[index];
      }

      private void InitRowGroupReaders()
      {
         _groupReaders.Clear();

         foreach(Thrift.RowGroup thriftRowGroup in _meta.Row_groups)
         {
            _groupReaders.Add(new ParquetRowGroupReader(thriftRowGroup, _footer, Stream, ThriftStream, _parquetOptions));
         }
      }

      /// <summary>
      /// Disposes 
      /// </summary>
      public void Dispose()
      {
      }
   }
}