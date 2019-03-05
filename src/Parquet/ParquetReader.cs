using Parquet.File;
using System;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;
using Parquet.Data.Rows;

namespace Parquet
{
   /// <summary>
   /// Implements Apache Parquet format reader, experimental version for next major release.
   /// </summary>
   public class ParquetReader : ParquetActor, IDisposable
   {
      private readonly Stream _input;
      private readonly Thrift.FileMetaData _meta;
      private readonly ThriftFooter _footer;
      private readonly ParquetOptions _parquetOptions;
      private readonly List<ParquetRowGroupReader> _groupReaders = new List<ParquetRowGroupReader>();
      private readonly bool _leaveStreamOpen;

      private ParquetReader(Stream input, bool leaveStreamOpen) : base(input)
      {
         _input = input ?? throw new ArgumentNullException(nameof(input));
         _leaveStreamOpen = leaveStreamOpen;
      }

      /// <summary>
      /// Creates an instance from input stream
      /// </summary>
      /// <param name="input">Input stream, must be readable and seekable</param>
      /// <param name="parquetOptions">Optional reader options</param>
      /// <param name="leaveStreamOpen">When true, leaves the stream passed in <paramref name="input"/> open after disposing the reader.</param>
      /// <exception cref="ArgumentNullException">input</exception>
      /// <exception cref="ArgumentException">stream must be readable and seekable - input</exception>
      /// <exception cref="IOException">not a Parquet file (size too small)</exception>
      public ParquetReader(Stream input, ParquetOptions parquetOptions = null, bool leaveStreamOpen = true) : this(input, leaveStreamOpen)
      {
         
         if (!input.CanRead || !input.CanSeek) throw new ArgumentException("stream must be readable and seekable", nameof(input));
         if (_input.Length <= 8) throw new IOException("not a Parquet file (size too small)");

         ValidateFile();
         _parquetOptions = parquetOptions ?? new ParquetOptions();

         //read metadata instantly, now
         _meta = ReadMetadata();
         _footer = new ThriftFooter(_meta);

         ParquetEventSource.Current.OpenStream(input.Length, leaveStreamOpen, _meta.Row_groups.Count, _meta.Num_rows);

         InitRowGroupReaders();
      }

      /// <summary>
      /// Opens reader from a file on disk. When the reader is disposed the file handle is automatically closed.
      /// </summary>
      /// <param name="filePath"></param>
      /// <param name="parquetOptions"></param>
      /// <returns></returns>
      public static ParquetReader OpenFromFile(string filePath, ParquetOptions parquetOptions = null)
      {
         Stream fs = System.IO.File.OpenRead(filePath);

         return new ParquetReader(fs, parquetOptions, false);
      }

      #region [ Helpers ]

      /// <summary>
      /// Reads entire file as a table
      /// </summary>
      public static Table ReadTableFromFile(string filePath, ParquetOptions parquetOptions = null)
      {
         using (ParquetReader reader = OpenFromFile(filePath, parquetOptions))
         {
            return reader.ReadAsTable();
         }
      }

      /// <summary>
      /// Reads entire stream as a table
      /// </summary>
      public static Table ReadTableFromStream(Stream stream, ParquetOptions parquetOptions = null)
      {
         using (var reader = new ParquetReader(stream, parquetOptions))
         {
            return reader.ReadAsTable();
         }
      }

      #endregion

      /// <summary>
      /// Gets the number of rows groups in this file
      /// </summary>
      public int RowGroupCount => _meta.Row_groups.Count;

      /// <summary>
      /// Reader schema
      /// </summary>
      public Schema Schema => _footer.CreateModelSchema(_parquetOptions);

      /// <summary>
      /// Internal parquet metadata
      /// </summary>
      public Thrift.FileMetaData ThriftMetadata => _meta;

      /// <summary>
      /// 
      /// </summary>
      /// <param name="index"></param>
      /// <returns></returns>
      public ParquetRowGroupReader OpenRowGroupReader(int index)
      {
         return _groupReaders[index];
      }

      /// <summary>
      /// Reads entire row group's data columns in one go.
      /// </summary>
      /// <param name="rowGroupIndex">Index of the row group. Default to the first row group if not specified.</param>
      /// <returns></returns>
      public DataColumn[] ReadEntireRowGroup(int rowGroupIndex = 0)
      {
         DataField[] dataFields = Schema.GetDataFields();
         DataColumn[] result = new DataColumn[dataFields.Length];

         using (ParquetRowGroupReader reader = OpenRowGroupReader(rowGroupIndex))
         {
            for (int i = 0; i < dataFields.Length; i++)
            {
               DataColumn column = reader.ReadColumn(dataFields[i]);
               result[i] = column;
            }
         }

         return result;
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
         if(!_leaveStreamOpen)
         {
            _input.Dispose();
         }
      }
   }
}