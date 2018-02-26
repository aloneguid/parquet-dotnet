using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Parquet.Data;

namespace Parquet.File
{
   internal class ParquetRowGroupWriter : IDisposable
   {
      private readonly Schema _schema;
      private readonly Stream _stream;
      private readonly ThriftStream _thriftStream;
      private readonly ThriftFooter _footer;
      private readonly int _rowCount;
      private readonly Thrift.RowGroup _thriftRowGroup;
      private readonly long _rgStartPos;
      private readonly List<Thrift.SchemaElement> _thschema;
      private int _colIdx;

      internal ParquetRowGroupWriter(Schema schema,
         Stream stream,
         ThriftStream thriftStream,
         ThriftFooter footer, 
         int rowCount)
      {
         _schema = schema ?? throw new ArgumentNullException(nameof(schema));
         _stream = stream ?? throw new ArgumentNullException(nameof(stream));
         _thriftStream = thriftStream ?? throw new ArgumentNullException(nameof(thriftStream));
         _footer = footer ?? throw new ArgumentNullException(nameof(footer));
         _rowCount = rowCount;

         _thriftRowGroup = _footer.AddRowGroup();
         _thriftRowGroup.Num_rows = _rowCount;
         _rgStartPos = _stream.Position;
         _thriftRowGroup.Columns = new List<Thrift.ColumnChunk>();
         _thschema = _footer.GetWriteableSchema().ToList();
      }

      public void Write(IEnumerable<bool> values)
      {

      }

      public void Write(IEnumerable<bool?> values)
      {

      }

      public void Write(IEnumerable<byte> values)
      {

      }

      public void Write(IEnumerable<byte?> values)
      {

      }

      public void Write(IEnumerable<int> values)
      {
         Thrift.SchemaElement tse = _thschema[_colIdx++];
         List<string> path = _footer.GetPath(tse);




      }

      public void Write(IEnumerable<int?> values)
      {

      }

      public void Write(IEnumerable<string> values)
      {

      }

      private void Write(DataColumn column)
      {

      }

      public void Dispose()
      {
         //row group's size is a sum of _uncompressed_ sizes of all columns in it, including the headers
         //luckily ColumnChunk already contains sizes of page+header in it's meta
         _thriftRowGroup.Total_byte_size = _thriftRowGroup.Columns.Sum(c => c.Meta_data.Total_compressed_size);
      }
   }
}