using System;
using System.IO;
using Parquet.Data;
using Parquet.File;

namespace Parquet
{
   /// <summary>
   /// vNext prototype
   /// </summary>
   internal class ParquetWriter3 : ParquetActor, IDisposable
   {
      private ThriftFooter _footer;
      private readonly Schema _schema;
      private readonly ParquetOptions _formatOptions;
      private readonly WriterOptions _writerOptions;
      private bool _dataWritten;

      /// <summary>
      /// Creates an instance of parquet writer on top of a stream
      /// </summary>
      /// <param name="schema"></param>
      /// <param name="output">Writeable, seekable stream</param>
      /// <param name="formatOptions">Additional options</param>
      /// <param name="writerOptions">The writer options.</param>
      /// <param name="append"></param>
      /// <exception cref="ArgumentNullException">Output is null.</exception>
      /// <exception cref="ArgumentException">Output stream is not writeable</exception>
      public ParquetWriter3(Schema schema, Stream output, ParquetOptions formatOptions = null, WriterOptions writerOptions = null, bool append = false)
         : base(new PositionTrackingStream(output))
      {
         if (output == null) throw new ArgumentNullException(nameof(output));

         if (!output.CanWrite) throw new ArgumentException("stream is not writeable", nameof(output));
         _schema = schema ?? throw new ArgumentNullException(nameof(schema));
         _formatOptions = formatOptions ?? new ParquetOptions();
         _writerOptions = writerOptions ?? new WriterOptions();

         PrepareFile(append);
      }

      public ParquetRowGroupWriter CreateRowGroup(int rowCount)
      {
         return new ParquetRowGroupWriter(_schema, Stream, ThriftStream, _footer, CompressionMethod.Snappy, rowCount);
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
            throw new ParquetException($"{nameof(DataSet)} schema does not match existing file schema, reason: {reason}");
         }
      }

      private void WriteMagic()
      {
         Stream.Write(MagicBytes, 0, MagicBytes.Length);
      }

      public void Dispose()
      {
         if (!_dataWritten) return;

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