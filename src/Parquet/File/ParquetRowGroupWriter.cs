using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Parquet.Data;
using Parquet.File.Values;

namespace Parquet.File
{
   internal class ParquetRowGroupWriter : IDisposable
   {
      private readonly Schema _schema;
      private readonly Stream _stream;
      private readonly ThriftStream _thriftStream;
      private readonly ThriftFooter _footer;
      private readonly CompressionMethod _compressionMethod;
      private readonly int _rowCount;
      private readonly Thrift.RowGroup _thriftRowGroup;
      private readonly long _rgStartPos;
      private readonly List<Thrift.SchemaElement> _thschema;
      private int _colIdx;

      private struct PageTag
      {
         public int HeaderSize;
         public Thrift.PageHeader HeaderMeta;
      }

      internal ParquetRowGroupWriter(Schema schema,
         Stream stream,
         ThriftStream thriftStream,
         ThriftFooter footer, 
         CompressionMethod compressionMethod,
         int rowCount)
      {
         _schema = schema ?? throw new ArgumentNullException(nameof(schema));
         _stream = stream ?? throw new ArgumentNullException(nameof(stream));
         _thriftStream = thriftStream ?? throw new ArgumentNullException(nameof(thriftStream));
         _footer = footer ?? throw new ArgumentNullException(nameof(footer));
         _compressionMethod = compressionMethod;
         _rowCount = rowCount;

         _thriftRowGroup = _footer.AddRowGroup();
         _thriftRowGroup.Num_rows = _rowCount;
         _rgStartPos = _stream.Position;
         _thriftRowGroup.Columns = new List<Thrift.ColumnChunk>();
         _thschema = _footer.GetWriteableSchema().ToList();
      }

      public void Write(DataColumn column)
      {
         if (column == null) throw new ArgumentNullException(nameof(column));

         Thrift.SchemaElement tse = _thschema[_colIdx++];
         List<string> path = _footer.GetPath(tse);

         //todo: add column chunk to row group after writing
         //Thrift.ColumnChunk chunk = cw.Write(offset, count, values);
         //rg.Columns.Add(chunk);
      }

      private Thrift.ColumnChunk WriteColumnChunk(Thrift.SchemaElement tse, List<string> path)
      {
         Thrift.ColumnChunk chunk = _footer.CreateColumnChunk(_compressionMethod, _stream, tse.Type, path, 0);
         Thrift.PageHeader ph = _footer.CreateDataPage(_rowCount);
         _footer.GetLevels(chunk, out int maxRepetitionLevel, out int maxDefinitionLevel);

         List<PageTag> pages = WriteColumn(null);

         chunk.Meta_data.Num_values = ph.Data_page_header.Num_values;

         //the following counters must include both data size and header size
         chunk.Meta_data.Total_compressed_size = pages.Sum(p => p.HeaderMeta.Compressed_page_size + p.HeaderSize);
         chunk.Meta_data.Total_uncompressed_size = pages.Sum(p => p.HeaderMeta.Uncompressed_page_size + p.HeaderSize);

         return chunk;
      }

      private List<PageTag> WriteColumn(DataColumn column)
      {
         //chain streams together so we have real streaming instead of wasting undefraggable LOH memory

         using (BinaryWriter writer = DataWriterFactory.CreateWriter(_stream, _compressionMethod))
         {
            
         }

         return null;
      }

      private void WriteLevels(BinaryWriter writer, List<int> levels, int maxLevel)
      {
         int bitWidth = maxLevel.GetBitWidth();
         RunLengthBitPackingHybridValuesWriter.Write(writer, bitWidth, levels);
      }


      public void Dispose()
      {
         //row group's size is a sum of _uncompressed_ sizes of all columns in it, including the headers
         //luckily ColumnChunk already contains sizes of page+header in it's meta
         _thriftRowGroup.Total_byte_size = _thriftRowGroup.Columns.Sum(c => c.Meta_data.Total_compressed_size);
      }
   }
}