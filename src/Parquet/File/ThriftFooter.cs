using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.Data;
using Parquet.DataTypes;
using Parquet.File.Data;

namespace Parquet.File
{
   class ThriftFooter
   {
      private readonly Thrift.FileMetaData _fileMeta;

      public ThriftFooter(Thrift.FileMetaData fileMeta)
      {
         _fileMeta = fileMeta;
      }

      public ThriftFooter(Schema schema)
      {
         throw new NotImplementedException();
      }

      public long Write(ThriftStream thriftStream)
      {
         return thriftStream.Write(_fileMeta);
      }

      public Thrift.SchemaElement GetSchemaElement(Thrift.ColumnChunk columnChunk)
      {
         if (columnChunk == null)
         {
            throw new ArgumentNullException(nameof(columnChunk));
         }

         List<string> path = columnChunk.Meta_data.Path_in_schema;

         int i = 0;
         foreach (string pp in path)
         {
            while(i < _fileMeta.Schema.Count)
            {
               if (_fileMeta.Schema[i].Name == pp) break;

               i++;
            }
         }

         return _fileMeta.Schema[i];
      }

      public void GetLevels(Thrift.ColumnChunk columnChunk, out int maxRepetitionLevel, out int maxDefinitionLevel)
      {
         maxRepetitionLevel = 0;
         maxDefinitionLevel = 0;

         int i = 0;
         List<string> path = columnChunk.Meta_data.Path_in_schema;

         foreach (string pp in path)
         {
            while(i < _fileMeta.Schema.Count)
            {
               if(_fileMeta.Schema[i].Name == pp)
               {
                  Thrift.SchemaElement se = _fileMeta.Schema[i];

                  bool repeated = (se.__isset.repetition_type && se.Repetition_type == Thrift.FieldRepetitionType.REPEATED);
                  bool defined = (se.Repetition_type != Thrift.FieldRepetitionType.REQUIRED);

                  if (repeated) maxRepetitionLevel += 1;
                  if (defined) maxDefinitionLevel += 1;

                  break;
               }

               i++;
            }
         }
      }

      public IEnumerable<Thrift.SchemaElement> GetWriteableSchema()
      {
         foreach(Thrift.SchemaElement tse in _fileMeta.Schema)
         {
            if(tse.__isset.type)
            {
               yield return tse;
            }
         }
      }

      public Thrift.RowGroup AddRowGroup()
      {
         var rg = new Thrift.RowGroup();
         _fileMeta.Row_groups.Add(rg);
         return rg;
      }

      public Thrift.ColumnChunk CreateColumnChunk(CompressionMethod compression, Stream output, Thrift.Type columnType, List<string> path, int valuesCount)
      {
         Thrift.CompressionCodec codec = DataFactory.GetThriftCompression(compression);

         var chunk = new Thrift.ColumnChunk();
         long startPos = output.Position;
         chunk.File_offset = startPos;
         chunk.Meta_data = new Thrift.ColumnMetaData();
         chunk.Meta_data.Num_values = valuesCount;
         chunk.Meta_data.Type = columnType;
         chunk.Meta_data.Codec = codec;
         chunk.Meta_data.Data_page_offset = startPos;
         chunk.Meta_data.Encodings = new List<Thrift.Encoding>
         {
            Thrift.Encoding.RLE,
            Thrift.Encoding.BIT_PACKED,
            Thrift.Encoding.PLAIN
         };
         chunk.Meta_data.Path_in_schema = path;

         return chunk;
      }

      public Thrift.PageHeader CreateDataPage(int valueCount)
      {
         var ph = new Thrift.PageHeader(Thrift.PageType.DATA_PAGE, 0, 0);
         ph.Data_page_header = new Thrift.DataPageHeader
         {
            Encoding = Thrift.Encoding.PLAIN,
            Definition_level_encoding = Thrift.Encoding.RLE,
            Repetition_level_encoding = Thrift.Encoding.RLE,
            Num_values = valueCount
         };

         return ph;
      }

      #region [ Conversion to Model Schema ]

      public Schema CreateModelSchema(ParquetOptions formatOptions)
      {
         int si = 0;
         Thrift.SchemaElement tse = _fileMeta.Schema[si++];
         var container = new List<SchemaElement>();

         CreateModelSchema(container, tse.Num_children, ref si, formatOptions);

         return new Schema(container);
      }

      private void CreateModelSchema(IList<SchemaElement> container, int childCount, ref int si, ParquetOptions formatOptions)
      {
         for (int i = 0; i < childCount && si < _fileMeta.Schema.Count; i++)
         {
            Thrift.SchemaElement tse = _fileMeta.Schema[si];
            IDataTypeHandler dth = DataTypeFactory.Match(tse, formatOptions);

            if (dth == null)
            {
               if (tse.Num_children > 0)
               {
                  var structure = new StructureSchemaElement(tse.Name, false);
                  container.Add(structure);
                  si++; //go deeper
                  CreateModelSchema(structure.Elements, tse.Num_children, ref si, formatOptions);
               }
               else
               {
                  ThrowNoHandler(tse);
               }
            }
            else
            {
               SchemaElement element = dth.CreateSchemaElement(_fileMeta.Schema, ref si);
               container.Add(element);

               if (element.DataType == DataType.List)
               {
                  ListSchemaElement lse = element as ListSchemaElement;
                  if (lse.Item == null)
                  {
                     tse = _fileMeta.Schema[si];
                     var vc = new List<SchemaElement>();
                     CreateModelSchema(vc, Math.Max(1, tse.Num_children), ref si, formatOptions);
                     lse.Item = vc.First();
                  }
               }
            }
         }
      }

      private void ThrowNoHandler(Thrift.SchemaElement tse)
      {
         string ct = tse.__isset.converted_type
            ? $" ({tse.Converted_type})"
            : null;

         string t = tse.__isset.type
            ? $"'{tse.Type}'"
            : "<unspecified>";

         throw new NotSupportedException($"cannot find data type handler for schema element '{tse.Name}' (type: {t}{ct})");
      }

      #endregion
   }
}
