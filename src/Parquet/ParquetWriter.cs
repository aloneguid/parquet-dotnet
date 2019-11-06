using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.Data;
using Parquet.File;
using Parquet.File.Streams;

namespace Parquet
{
   /// <summary>
   /// Implements Apache Parquet format writer
   /// </summary>
   public class ParquetWriter : ParquetActor, IDisposable
   {
      private ThriftFooter _footer;
      private readonly Schema _schema;
      private readonly ParquetOptions _formatOptions;
      private bool _dataWritten;
      private readonly List<ParquetRowGroupWriter> _openedWriters = new List<ParquetRowGroupWriter>();

      /// <summary>
      /// Type of compression to use, defaults to <see cref="CompressionMethod.Snappy"/>
      /// </summary>
      public CompressionMethod CompressionMethod { get; set; } = CompressionMethod.Snappy;

      /// <summary>
      /// Creates an instance of parquet writer on top of a stream
      /// </summary>
      /// <param name="schema"></param>
      /// <param name="output">Writeable, seekable stream</param>
      /// <param name="formatOptions">Additional options</param>
      /// <param name="append"></param>
      /// <exception cref="ArgumentNullException">Output is null.</exception>
      /// <exception cref="ArgumentException">Output stream is not writeable</exception>
      public ParquetWriter(Schema schema, Stream output, ParquetOptions formatOptions = null, bool append = false)
         : base(new GapStream(output))
      {
         if (output == null) throw new ArgumentNullException(nameof(output));

         if (!output.CanWrite) throw new ArgumentException("stream is not writeable", nameof(output));
         _schema = schema ?? throw new ArgumentNullException(nameof(schema));
         _formatOptions = formatOptions ?? new ParquetOptions();

         PrepareFile(append);
      }

      /// <summary>
      /// Creates a new row group and a writer for it.
      /// </summary>
      public ParquetRowGroupWriter CreateRowGroup()
      {
         _dataWritten = true;

         var writer = new ParquetRowGroupWriter(_schema, Stream, ThriftStream, _footer, CompressionMethod, _formatOptions);

         _openedWriters.Add(writer);

         return writer;
      }

      private void PrepareFile(bool append)
      {
         if (append)
         {
            if (!Stream.CanSeek) throw new IOException("destination stream must be seekable for append operations.");

            ValidateFile();

            Thrift.FileMetaData fileMeta = ReadMetadata();
            _footer = new ThriftFooter(fileMeta);

            ValidateSchemasCompatible(_footer, _schema);

            GoBeforeFooter();
         }
         else
         {
            if (_footer == null)
            {
               _footer = new ThriftFooter(_schema, 0 /* todo: don't forget to set the total row count at the end!!! */);

               //file starts with magic
               WriteMagic();
            }
            else
            {
               ValidateSchemasCompatible(_footer, _schema);

               _footer.Add(0 /* todo: don't forget to set the total row count at the end!!! */);
            }
         }
      }

      private void ValidateSchemasCompatible(ThriftFooter footer, Schema schema)
      {
         Schema existingSchema = footer.CreateModelSchema(_formatOptions);

         if (!schema.Equals(existingSchema))
         {
            string reason = schema.GetNotEqualsMessage(existingSchema, "appending", "existing");
            throw new ParquetException($"passed schema does not match existing file schema, reason: {reason}");
         }
      }

      private void WriteMagic()
      {
         Stream.Write(MagicBytes, 0, MagicBytes.Length);
      }

      /// <summary>
      /// Disposes the writer and writes the file footer.
      /// </summary>
      public void Dispose()
      {
         if (_dataWritten)
         {
            //update row count (on append add row count to existing metadata)
            _footer.Add(_openedWriters.Sum(w => w.RowCount ?? 0));
         }

         //finalize file
         long size = _footer.Write(ThriftStream);

         //metadata size
         Writer.Write((int)size);  //4 bytes

         //end magic
         WriteMagic();              //4 bytes

         Writer.Flush();
         Stream.Flush();
      }
   }
}