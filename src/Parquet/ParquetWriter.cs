using System;
using System.IO;
using Parquet.File;
using System.Collections.Generic;
using System.Linq;
using System.Collections;
using Parquet.Data;

namespace Parquet
{
   /// <summary>
   /// Implements Apache Parquet format writer
   /// </summary>
   public class ParquetWriter : ParquetActor, IDisposable
   {
      private readonly FileMetadataBuilder _meta;
      private readonly ParquetOptions _formatOptions;
      private readonly WriterOptions _writerOptions;
      private Schema _existingSchema;
      private bool _dataWritten;

      /// <summary>
      /// Creates an instance of parquet writer on top of a stream
      /// </summary>
      /// <param name="output">Writeable, seekable stream</param>
      /// <param name="formatOptions">Additional options</param>
      /// <param name="writerOptions">The writer options.</param>
      /// <exception cref="ArgumentNullException">Output is null.</exception>
      /// <exception cref="ArgumentException">Output stream is not writeable</exception>
      public ParquetWriter(Stream output, ParquetOptions formatOptions = null, WriterOptions writerOptions = null) 
         : base(new PositionTrackingStream(output))
      {
         if (output == null) throw new ArgumentNullException(nameof(output));

         if (!output.CanWrite) throw new ArgumentException("stream is not writeable", nameof(output));
         _formatOptions = formatOptions ?? new ParquetOptions();
         _writerOptions = writerOptions ?? new WriterOptions();
         _meta = new FileMetadataBuilder();
      }

      /// <summary>
      /// Write out dataset to the output stream
      /// </summary>
      /// <param name="dataSet">Dataset to write</param>
      /// <param name="compression">Compression method</param>
      /// <param name="append">When true, appends to the file, otherwise creates a new file.</param>
      public void Write(DataSet dataSet, CompressionMethod compression = CompressionMethod.Gzip, bool append = false)
      {
         PrepareFile(dataSet, append);

         int offset = 0;
         int count;
         do
         {
            count = Math.Min(_writerOptions.RowGroupsSize, dataSet.Count - offset);
            Thrift.RowGroup rg = _meta.AddRowGroup();
            long rgStartPos = Stream.Position;

            rg.Columns = new List<Thrift.ColumnChunk>();
            foreach(SchemaElement se in dataSet.Schema.Flatten())
            {
               var cw = new ColumnWriter(Stream, ThriftStream, _meta, se, compression, _formatOptions, _writerOptions);
               IList values = dataSet.GetColumn(se, offset, count);
               Thrift.ColumnChunk chunk = cw.Write(offset, count, values);
               rg.Columns.Add(chunk);
            }

            //row group's size is a sum of _uncompressed_ sizes of all columns in it, including the headers
            //luckily ColumnChunk already contains sizes of page+header in it's meta
            rg.Total_byte_size = rg.Columns.Sum(c => c.Meta_data.Total_compressed_size);
            rg.Num_rows = count;

            offset += _writerOptions.RowGroupsSize;
         }
         while (offset < dataSet.Count);

         _dataWritten = true;
      }

      private void PrepareFile(DataSet ds, bool append)
      {
         if (append)
         {
            if (!Stream.CanSeek) throw new IOException("destination stream must be seekable for append operations.");

            ValidateFile();

            Thrift.FileMetaData fileMeta = ReadMetadata();
            _meta.SetMeta(fileMeta);

            Schema existingSchema = new FileMetadataParser(fileMeta).ParseSchema(_formatOptions);

            if (!ds.Schema.Equals(existingSchema))
            {
               throw new ParquetException($"{nameof(DataSet)} schema does not match existing file schema");
            }

            GoBeforeFooter();
         }
         else
         {
            if (_existingSchema == null)
            {
               _existingSchema = ds.Schema;

               //file starts with magic
               WriteMagic();

               _meta.AddSchema(ds);
            }
            else
            {
               if(!_existingSchema.Equals(ds.Schema))
               {
                  throw new ParquetException($"expeted schema {_existingSchema} but found {ds.Schema}.");
               }
            }
         }
      }

      /// <summary>
      /// Writes <see cref="DataSet"/> to a target stream
      /// </summary>
      /// <param name="dataSet"><see cref="DataSet"/> to write</param>
      /// <param name="destination">Destination stream</param>
      /// <param name="compression">Compression method</param>
      /// <param name="formatOptions">Parquet options, optional.</param>
      /// <param name="writerOptions">Writer options, optional.</param>
      /// <param name="append">When true, assumes that this stream contains existing file and appends data to it, otherwise writes a new Parquet file.</param>
      public static void Write(DataSet dataSet, Stream destination, CompressionMethod compression = CompressionMethod.Gzip, ParquetOptions formatOptions = null, WriterOptions writerOptions = null, bool append = false)
      {
         using (var writer = new ParquetWriter(destination, formatOptions, writerOptions))
         {
            writer.Write(dataSet, compression, append);
         }
      }

      /// <summary>
      /// Writes <see cref="DataSet"/> to a target file
      /// </summary>
      /// <param name="dataSet"><see cref="DataSet"/> to write</param>
      /// <param name="fileName">Path to a file to write to.</param>
      /// <param name="compression">Compression method</param>
      /// <param name="formatOptions">Parquet options, optional.</param>
      /// <param name="writerOptions">Writer options, optional.</param>
      /// <param name="append">When true, assumes that this stream contains existing file and appends data to it, otherwise writes a new Parquet file.</param>
      public static void WriteFile(DataSet dataSet, string fileName, CompressionMethod compression = CompressionMethod.Gzip, ParquetOptions formatOptions = null, WriterOptions writerOptions = null, bool append = false)
      {
         using (Stream fs = System.IO.File.Create(fileName))
         {
            using (var writer = new ParquetWriter(fs, formatOptions, writerOptions))
            {
               writer.Write(dataSet, compression);
            }
         }
      }

      private void WriteMagic()
      {
         Stream.Write(MagicBytes, 0, MagicBytes.Length);
      }

      /// <summary>
      /// Finalizes file, writes metadata and footer
      /// </summary>
      public void Dispose()
      {
         if (!_dataWritten) return;

         //finalize file
         long size = ThriftStream.Write(_meta.ThriftMeta);

         //metadata size
         Writer.Write((int)size);  //4 bytes

         //end magic
         WriteMagic();              //4 bytes

         Writer.Flush();
         Stream.Flush();
      }
   }
}
