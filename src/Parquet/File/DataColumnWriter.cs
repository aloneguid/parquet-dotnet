﻿using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Parquet.Data;
using Parquet.File.Streams;
using Parquet.File.Values;

namespace Parquet.File
{
   class DataColumnWriter
   {
      private readonly Stream _stream;
      private readonly ThriftStream _thriftStream;
      private readonly ThriftFooter _footer;
      private readonly Thrift.SchemaElement _schemaElement;
      private readonly CompressionMethod _compressionMethod;
      private readonly int _compressionLevel;
      private readonly int _rowCount;

      private struct PageTag
      {
         public int HeaderSize;
         public Thrift.PageHeader HeaderMeta;
      }

      public DataColumnWriter(
         Stream stream,
         ThriftStream thriftStream,
         ThriftFooter footer,
         Thrift.SchemaElement schemaElement,
         CompressionMethod compressionMethod,
         int compressionLevel,
         int rowCount)
      {
         _stream = stream;
         _thriftStream = thriftStream;
         _footer = footer;
         _schemaElement = schemaElement;
         _compressionMethod = compressionMethod;
         _compressionLevel = compressionLevel;
         _rowCount = rowCount;
      }

      public Thrift.ColumnChunk Write(List<string> path, DataColumn column, IDataTypeHandler dataTypeHandler)
      {
         Thrift.ColumnChunk chunk = _footer.CreateColumnChunk(_compressionMethod, _stream, _schemaElement.Type, path, 0);
         Thrift.PageHeader ph = _footer.CreateDataPage(column.Data.Length);
         _footer.GetLevels(chunk, out int maxRepetitionLevel, out int maxDefinitionLevel);

         List<PageTag> pages = WriteColumn(column, _schemaElement, dataTypeHandler, maxRepetitionLevel, maxDefinitionLevel);
         //generate stats for column chunk
         chunk.Meta_data.Statistics = column.Statistics.ToThriftStatistics(dataTypeHandler, _schemaElement);

         //this count must be set to number of all values in the column, including nulls.
         //for hierarchy/repeated columns this is a count of flattened list, including nulls.
         chunk.Meta_data.Num_values = ph.Data_page_header.Num_values;
         ph.Data_page_header.Statistics = chunk.Meta_data.Statistics;   //simply copy statistics to page header

         //the following counters must include both data size and header size
         chunk.Meta_data.Total_compressed_size = pages.Sum(p => p.HeaderMeta.Compressed_page_size + p.HeaderSize);
         chunk.Meta_data.Total_uncompressed_size = pages.Sum(p => p.HeaderMeta.Uncompressed_page_size + p.HeaderSize);

         return chunk;
      }

      private List<PageTag> WriteColumn(DataColumn column,
         Thrift.SchemaElement tse,
         IDataTypeHandler dataTypeHandler,
         int maxRepetitionLevel,
         int maxDefinitionLevel)
      {
         var pages = new List<PageTag>();

         /*
          * Page header must preceeed actual data (compressed or not) however it contains both
          * the uncompressed and compressed data size which we don't know! This somehow limits
          * the write efficiency.
          */


         using (var ms = new MemoryStream())
         {
            Thrift.PageHeader dataPageHeader = _footer.CreateDataPage(column.Data.Length);

            //chain streams together so we have real streaming instead of wasting undefraggable LOH memory
            using (GapStream pageStream = DataStreamFactory.CreateWriter(ms, _compressionMethod, _compressionLevel, true))
            {
               using (var writer = new BinaryWriter(pageStream, Encoding.UTF8, true))
               {
                  if (column.RepetitionLevels != null)
                  {
                     WriteLevels(writer, column.RepetitionLevels, column.RepetitionLevels.Length, maxRepetitionLevel);
                  }

                  Array data = column.Data;

                  if (maxDefinitionLevel > 0)
                  {
                     data = column.PackDefinitions(maxDefinitionLevel, out int[] definitionLevels, out int definitionLevelsLength, out int nullCount);

                     //last chance to capture null count as null data is compressed now
                     column.Statistics.NullCount = nullCount;

                     try
                     {
                        WriteLevels(writer, definitionLevels, definitionLevelsLength, maxDefinitionLevel);
                     }
                     finally
                     {
                        if (definitionLevels != null)
                        {
                           ArrayPool<int>.Shared.Return(definitionLevels);
                        }
                     }
                  }
                  else
                  {
                     //no defitions means no nulls
                     column.Statistics.NullCount = 0;
                  }

                  dataTypeHandler.Write(tse, writer, data, column.Statistics);

                  writer.Flush();
               }

               pageStream.Flush();   //extremely important to flush the stream as some compression algorithms don't finish writing
               pageStream.MarkWriteFinished();
               dataPageHeader.Uncompressed_page_size = (int)pageStream.Position;
            }
            dataPageHeader.Compressed_page_size = (int)ms.Position;

            //write the header in
            dataPageHeader.Data_page_header.Statistics = column.Statistics.ToThriftStatistics(dataTypeHandler, _schemaElement);
            int headerSize = _thriftStream.Write(dataPageHeader);
            ms.Position = 0;
            ms.CopyTo(_stream);


            var dataTag = new PageTag
            {
               HeaderMeta = dataPageHeader,
               HeaderSize = headerSize
            };

            pages.Add(dataTag);
         }

         return pages;
      }

      private void WriteLevels(BinaryWriter writer, int[] levels, int count, int maxLevel)
      {
         int bitWidth = maxLevel.GetBitWidth();
         RunLengthBitPackingHybridValuesWriter.WriteForwardOnly(writer, bitWidth, levels, count);
      }
   }
}