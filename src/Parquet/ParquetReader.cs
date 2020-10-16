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
      /// <summary>
      /// In order to reduce the number of same-string
      /// allocations, a running cache of recent strings
      /// for a single column can be maintained and when
      /// the same string is referenced again, the already
      /// allocated string will be reused.
      ///
      /// This changes the look-back of how many strings to cache.
      /// There is an inflection point where it will slow down parsing
      /// as the match is a linear search through an array.
      /// However, larger values will reduce memory if there
      /// are many scattered common strings. A value of 1-5 is very
      /// fast but will only catch the strings which repeat in a long series.
      /// A value of 100 slows down but is a much more aggressive string reuse
      /// and can reduce memory consumption.
      ///
      /// Default is 50 -- It gives good a good speed improvement. A value of 5
      /// will eek out slightly better performance if you have long runs of repeated
      /// strings.
      /// </summary>
      public static int StringCacheSize = 50;

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

      /// <summary>
      /// Gets custom key-value pairs for metadata
      /// </summary>
      public Dictionary<string, string> CustomMetadata => _footer.CustomMetadata;


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