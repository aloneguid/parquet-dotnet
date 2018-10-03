using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.Data;
using Parquet.File;
using Parquet.File.Values;

namespace Parquet
{
   /// <summary>
   /// Writer for Parquet row groups
   /// </summary>
   public class ParquetRowGroupWriter : IDisposable
   {
      private readonly Schema _schema;
      private readonly Stream _stream;
      private readonly ThriftStream _thriftStream;
      private readonly ThriftFooter _footer;
      private readonly CompressionMethod _compressionMethod;
      private readonly ParquetOptions _formatOptions;
      private readonly Thrift.RowGroup _thriftRowGroup;
      private readonly long _rgStartPos;
      private readonly Thrift.SchemaElement[] _thschema;
      private int _colIdx;

      internal ParquetRowGroupWriter(Schema schema,
         Stream stream,
         ThriftStream thriftStream,
         ThriftFooter footer, 
         CompressionMethod compressionMethod,
         ParquetOptions formatOptions)
      {
         _schema = schema ?? throw new ArgumentNullException(nameof(schema));
         _stream = stream ?? throw new ArgumentNullException(nameof(stream));
         _thriftStream = thriftStream ?? throw new ArgumentNullException(nameof(thriftStream));
         _footer = footer ?? throw new ArgumentNullException(nameof(footer));
         _compressionMethod = compressionMethod;
         _formatOptions = formatOptions;

         _thriftRowGroup = _footer.AddRowGroup();
         _rgStartPos = _stream.Position;
         _thriftRowGroup.Columns = new List<Thrift.ColumnChunk>();
         _thschema = _footer.GetWriteableSchema();
      }

      internal long? RowCount { get; private set; }

      /// <summary>
      /// Writes next data column to parquet stream. Note that columns must be written in the order they are declared in the
      /// file schema.
      /// </summary>
      /// <param name="column"></param>
      public void WriteColumn(DataColumn column)
      {
         if (column == null) throw new ArgumentNullException(nameof(column));

         if (RowCount == null)
         {
            RowCount = column.CalculateRowCount();
         }

         Thrift.SchemaElement tse = _thschema[_colIdx];
         if(!column.Field.Equals(tse))
         {
            throw new ArgumentException($"cannot write this column, expected '{tse.Name}', passed: '{column.Field.Name}'", nameof(column));
         }
         IDataTypeHandler dataTypeHandler = DataTypeFactory.Match(tse, _formatOptions);
         _colIdx += 1;

         List<string> path = _footer.GetPath(tse);

         var writer = new DataColumnWriter(_stream, _thriftStream, _footer, tse, _compressionMethod, (int)RowCount.Value);

         Thrift.ColumnChunk chunk = writer.Write(path, column, dataTypeHandler);
         _thriftRowGroup.Columns.Add(chunk);

      }

      /// <summary>
      /// 
      /// </summary>
      public void Dispose()
      {
         //todo: check if all columns are present

         //row count is know only after at least one column is written
         _thriftRowGroup.Num_rows = RowCount ?? 0;

         //row group's size is a sum of _uncompressed_ sizes of all columns in it, including the headers
         //luckily ColumnChunk already contains sizes of page+header in it's meta
         _thriftRowGroup.Total_byte_size = _thriftRowGroup.Columns.Sum(c => c.Meta_data.Total_compressed_size);
      }
   }
}