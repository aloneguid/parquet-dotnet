using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Parquet.Data;
using Parquet.File.Data;

namespace Parquet.File
{
   class ThriftFooter
   {
      private readonly Thrift.FileMetaData _fileMeta;

      public ThriftFooter(Thrift.FileMetaData fileMeta)
      {
         _fileMeta = fileMeta ?? throw new ArgumentNullException(nameof(fileMeta));
      }

      public ThriftFooter(Schema schema, long totalRowCount)
      {
         if (schema == null)
         {
            throw new ArgumentNullException(nameof(schema));
         }

         _fileMeta = CreateThriftSchema(schema);
         _fileMeta.Num_rows = totalRowCount;

         _fileMeta.Created_by = $"Parquet.Net version %Version% (build %Git.LongCommitHash%)";
      }

      public Dictionary<string, string> CustomMetadata
      {
         set
         {
            _fileMeta.Key_value_metadata = null;
            if (value == null || value.Count == 0) return;

            _fileMeta.Key_value_metadata = value
               .Select(kvp => new Thrift.KeyValue(kvp.Key) { Value = kvp.Value })
               .ToList();
         }
         get
         {
            if (_fileMeta.Key_value_metadata == null || _fileMeta.Key_value_metadata.Count == 0) return null;

            return _fileMeta.Key_value_metadata.ToDictionary(kv => kv.Key, kv => kv.Value);
         }
      }

      public void Add(long totalRowCount)
      {
         _fileMeta.Num_rows += totalRowCount;
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

      public List<string> GetPath(Thrift.SchemaElement schemaElement)
      {
         var tree = new ThriftSchemaTree(_fileMeta.Schema);
         var path = new List<string>();

         ThriftSchemaTree.Node wrapped = tree.Find(schemaElement);
         while(wrapped.parent != null)
         {
            path.Add(wrapped.element.Name);
            wrapped = wrapped.parent;
         }

         path.Reverse();
         return path;
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
                  bool defined = (se.Repetition_type == Thrift.FieldRepetitionType.REQUIRED);

                  if (repeated) maxRepetitionLevel += 1;
                  if (!defined) maxDefinitionLevel += 1;

                  break;
               }

               i++;
            }
         }
      }

      public Thrift.SchemaElement[] GetWriteableSchema()
      {
         return _fileMeta.Schema.Where(tse => tse.__isset.type).ToArray();
      }

      public Thrift.RowGroup AddRowGroup()
      {
         var rg = new Thrift.RowGroup();
         if (_fileMeta.Row_groups == null) _fileMeta.Row_groups = new List<Thrift.RowGroup>();
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
         chunk.Meta_data.Statistics = new Thrift.Statistics();

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
         var container = new List<Field>();

         CreateModelSchema(null, container, tse.Num_children, ref si, formatOptions);

         return new Schema(container);
      }

      private void CreateModelSchema(string path, IList<Field> container, int childCount, ref int si, ParquetOptions formatOptions)
      {
         for (int i = 0; i < childCount && si < _fileMeta.Schema.Count; i++)
         {
            Thrift.SchemaElement tse = _fileMeta.Schema[si];
            IDataTypeHandler dth = DataTypeFactory.Match(tse, formatOptions);

            if(dth == null)
            {
               throw new InvalidOperationException($"cannot find data type handler to create model schema for {tse.Describe()}");
            }

            Field se = dth.CreateSchemaElement(_fileMeta.Schema, ref si, out int ownedChildCount);

            se.Path = string.Join(Schema.PathSeparator, new[] { path, se.Path ?? se.Name }.Where(p => p != null));

            if (ownedChildCount > 0)
            {
               var childContainer = new List<Field>();
               CreateModelSchema(se.Path, childContainer, ownedChildCount, ref si, formatOptions);
               foreach(Field cse in childContainer)
               {
                  se.Assign(cse);
               }
            }


            container.Add(se);
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

      #region [ Convertion from Model Schema ]

      public Thrift.FileMetaData CreateThriftSchema(Schema schema)
      {
         var meta = new Thrift.FileMetaData();
         meta.Version = 1;
         meta.Schema = new List<Thrift.SchemaElement>();

         Thrift.SchemaElement root = AddRoot(meta.Schema);
         CreateThriftSchema(schema.Fields, root, meta.Schema);

         return meta;
      }

      private Thrift.SchemaElement AddRoot(IList<Thrift.SchemaElement> container)
      {
         var root = new Thrift.SchemaElement("parquet-dotnet-schema");
         container.Add(root);
         return root;
      }

      private void CreateThriftSchema(IEnumerable<Field> ses, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
      {
         foreach(Field se in ses)
         {
            IDataTypeHandler handler = DataTypeFactory.Match(se);

            //todo: check that handler is found indeed

            handler.CreateThrift(se, parent, container);
         }
      }

      #endregion

      #region [ Helpers ]

      class ThriftSchemaTree
      {
         public class Node
         {
            public Thrift.SchemaElement element;
            public List<Node> children;
            public Node parent;
         }

         public Node root;

         public ThriftSchemaTree(List<Thrift.SchemaElement> schema)
         {
            root = new Node { element = schema[0] };
            int i = 1;

            BuildSchema(root, schema, root.element.Num_children, ref i);
         }

         public Node Find(Thrift.SchemaElement tse)
         {
            return Find(root, tse);
         }

         private Node Find(Node root, Thrift.SchemaElement tse)
         {
            foreach(Node child in root.children)
            {
               if (child.element == tse) return child;

               if(child.children != null)
               {
                  Node cf = Find(child, tse);
                  if (cf != null) return cf;
               }
            }

            return null;
         }

         private void BuildSchema(Node parent, List<Thrift.SchemaElement> schema, int count, ref int i)
         {
            parent.children = new List<Node>();
            for(int ic = 0; ic < count; ic++)
            {
               Thrift.SchemaElement child = schema[i++];
               var node = new Node { element = child, parent = parent };
               parent.children.Add(node);
               if(child.Num_children > 0)
               {
                  BuildSchema(node, schema, child.Num_children, ref i);
               }
            }
         }
      }

      #endregion
   }
}
