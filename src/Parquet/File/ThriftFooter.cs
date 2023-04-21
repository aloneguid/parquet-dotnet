using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Encodings;
using Parquet.Schema;
using Parquet.Thrift;

namespace Parquet.File {
    class ThriftFooter {
        private readonly Thrift.FileMetaData _fileMeta;
        private readonly ThriftSchemaTree _tree;

        internal static ThriftFooter Empty => new();

        internal ThriftFooter() {
            _fileMeta = new Thrift.FileMetaData();
            _tree= new ThriftSchemaTree();
        }

        public ThriftFooter(Thrift.FileMetaData fileMeta) {
            _fileMeta = fileMeta ?? throw new ArgumentNullException(nameof(fileMeta));
            _tree = new ThriftSchemaTree(_fileMeta.Schema);
        }

        internal static ParquetSchema Parse(params Thrift.SchemaElement[] elements) {

            var slst = new List<Thrift.SchemaElement> {
                new Thrift.SchemaElement("root") { Num_children = 1 },
            };
            slst.AddRange(elements);

            return new ThriftFooter(new Thrift.FileMetaData {
                Schema = slst
            }).CreateModelSchema(new ParquetOptions());
        }

        public ThriftFooter(ParquetSchema schema, long totalRowCount) {
            if(schema == null) {
                throw new ArgumentNullException(nameof(schema));
            }

            _fileMeta = CreateThriftSchema(schema);
            _fileMeta.Num_rows = totalRowCount;


            // Looks like Spark is sensitive about this format. See https://github.com/aloneguid/parquet-dotnet/issues/261
#if DEBUG
            _fileMeta.Created_by = "Parquet.Net version LocalDev (build Local)";
#else
            _fileMeta.Created_by = $"Parquet.Net version {Globals.Version} (build {Globals.GithubSha})";
#endif
            _tree = new ThriftSchemaTree(_fileMeta.Schema);
        }

        public Dictionary<string, string> CustomMetadata {
            set {
                _fileMeta.Key_value_metadata = null;
                if(value == null || value.Count == 0)
                    return;

                _fileMeta.Key_value_metadata = value
                   .Select(kvp => new Thrift.KeyValue(kvp.Key) { Value = kvp.Value })
                   .ToList();
            }
            get {
                if(_fileMeta.Key_value_metadata == null || _fileMeta.Key_value_metadata.Count == 0)
                    return new Dictionary<string, string>();

                return _fileMeta.Key_value_metadata.ToDictionary(kv => kv.Key, kv => kv.Value);
            }
        }

        public void Add(long totalRowCount) {
            _fileMeta.Num_rows += totalRowCount;
        }

        public async Task<long> WriteAsync(ThriftStream thriftStream, CancellationToken cancellationToken = default) {
            return await thriftStream.WriteAsync(_fileMeta, false, cancellationToken);
        }

        public Thrift.SchemaElement? GetSchemaElement(Thrift.ColumnChunk columnChunk) {
            if(columnChunk == null) {
                throw new ArgumentNullException(nameof(columnChunk));
            }

            var findPath = new FieldPath(columnChunk.Meta_data.Path_in_schema);
            return _tree.Find(findPath)?.element;
        }

        public FieldPath GetPath(Thrift.SchemaElement schemaElement) {
            var path = new List<string>();

            ThriftSchemaTree.Node? wrapped = _tree.Find(schemaElement);
            while(wrapped?.parent != null) {
                string? name = wrapped.element?.Name;
                if(name != null)
                    path.Add(name);
                wrapped = wrapped.parent;
            }

            path.Reverse();
            return new FieldPath(path);
        }

        public Thrift.SchemaElement[] GetWriteableSchema() {
            return _fileMeta.Schema.Where(tse => tse.__isset.type).ToArray();
        }

        public Thrift.RowGroup AddRowGroup() {
            var rg = new Thrift.RowGroup();
            _fileMeta.Row_groups ??= new List<Thrift.RowGroup>();
            _fileMeta.Row_groups.Add(rg);
            return rg;
        }

        public Thrift.ColumnChunk CreateColumnChunk(CompressionMethod compression, System.IO.Stream output,
            Thrift.Type columnType, FieldPath path, int valuesCount) {
            CompressionCodec codec = (Thrift.CompressionCodec)(int)compression;

            var chunk = new Thrift.ColumnChunk();
            long startPos = output.Position;
            chunk.File_offset = startPos;
            chunk.Meta_data = new Thrift.ColumnMetaData();
            chunk.Meta_data.Num_values = valuesCount;
            chunk.Meta_data.Type = columnType;
            chunk.Meta_data.Codec = codec;
            chunk.Meta_data.Data_page_offset = startPos;
            chunk.Meta_data.Encodings = new List<Thrift.Encoding> {
                Thrift.Encoding.RLE,
                Thrift.Encoding.BIT_PACKED,
                Thrift.Encoding.PLAIN
            };
            chunk.Meta_data.Path_in_schema = path.ToList();
            chunk.Meta_data.Statistics = new Thrift.Statistics();

            return chunk;
        }

        public Thrift.PageHeader CreateDataPage(int valueCount, bool isDictionary) => 
            new Thrift.PageHeader(Thrift.PageType.DATA_PAGE, 0, 0) {
                Data_page_header = new Thrift.DataPageHeader {
                    Encoding = isDictionary ? Thrift.Encoding.PLAIN_DICTIONARY : Thrift.Encoding.PLAIN,
                    Definition_level_encoding = Thrift.Encoding.RLE,
                    Repetition_level_encoding = Thrift.Encoding.RLE,
                    Num_values = valueCount,
                    Statistics = new Thrift.Statistics()
                }
            };

        public Thrift.PageHeader CreateDictionaryPage(int numValues) {
            var ph = new Thrift.PageHeader(Thrift.PageType.DICTIONARY_PAGE, 0, 0);
            ph.Dictionary_page_header = new DictionaryPageHeader {
                Encoding = Thrift.Encoding.PLAIN_DICTIONARY,
                Num_values = numValues
            };
            return ph;
        }

#region [ Conversion to Model Schema ]

        public ParquetSchema CreateModelSchema(ParquetOptions formatOptions) {
            int si = 0;
            Thrift.SchemaElement tse = _fileMeta.Schema[si++];
            var container = new List<Field>();

            CreateModelSchema(null, container, tse.Num_children, ref si, formatOptions);

            return new ParquetSchema(container);
        }

        private void CreateModelSchema(FieldPath? path, IList<Field> container, int childCount, ref int si, ParquetOptions formatOptions) {
            for(int i = 0; i < childCount && si < _fileMeta.Schema.Count; i++) {
                Field? se = SchemaEncoder.Decode(_fileMeta.Schema, formatOptions, ref si, out int ownedChildCount);
                if(se == null)
                    throw new InvalidOperationException($"cannot decode schema for field {_fileMeta.Schema[si]}");

                List<string> npath = path?.ToList() ?? new List<string>();
                if(se.Path != null) npath.AddRange(se.Path.ToList());
                else npath.Add(se.Name);
                se.Path = new FieldPath(npath);

                if(ownedChildCount > 0) {
                    var childContainer = new List<Field>();
                    CreateModelSchema(se.Path, childContainer, ownedChildCount, ref si, formatOptions);
                    foreach(Field cse in childContainer) {
                        se.Assign(cse);
                    }
                }

                container.Add(se);
            }
        }

#endregion

#region [ Convertion from Model Schema ]

        public Thrift.FileMetaData CreateThriftSchema(ParquetSchema schema) {
            var meta = new Thrift.FileMetaData();
            meta.Version = 1;
            meta.Schema = new List<Thrift.SchemaElement>();
            meta.Row_groups = new List<Thrift.RowGroup>();

            Thrift.SchemaElement root = ThriftFooter.AddRoot(meta.Schema);
            foreach(Field se in schema.Fields) {
                SchemaEncoder.Encode(se, root, meta.Schema);
            }

            return meta;
        }


        private static Thrift.SchemaElement AddRoot(IList<Thrift.SchemaElement> container) {
            var root = new Thrift.SchemaElement("root");
            container.Add(root);
            return root;
        }

#endregion

#region [ Helpers ]

        class ThriftSchemaTree {
            readonly Dictionary<SchemaElement, Node?> _memoizedFindResults = 
                new Dictionary<SchemaElement, Node?>(new ReferenceEqualityComparer<SchemaElement>());

            public class Node {
                public Thrift.SchemaElement? element;
                public List<Node>? children;
                public Node? parent;
            }

            public Node root;

            internal ThriftSchemaTree() {
                root = new Node();
            }

            public ThriftSchemaTree(List<Thrift.SchemaElement> schema) {
                root = new Node { element = schema[0] };
                int i = 1;

                BuildSchema(root, schema, root.element.Num_children, ref i);
            }

            public Node? Find(Thrift.SchemaElement tse) {
                if(_memoizedFindResults.TryGetValue(tse, out Node? node)) {
                    return node;
                }
                node = Find(root, tse);
                _memoizedFindResults.Add(tse, node);
                return node;
            }

            private Node? Find(Node root, Thrift.SchemaElement tse) {
                if(root.children != null) {
                    foreach(Node child in root.children) {
                        if(child.element == tse)
                            return child;

                        if(child.children != null) {
                            Node? cf = Find(child, tse);
                            if(cf != null)
                                return cf;
                        }
                    }
                }

                return null;
            }

            public Node? Find(FieldPath path) {
                if(path.Length == 0) return null;
                return Find(root, path);
            }

            private Node? Find(Node root, FieldPath path) {
                if(root.children != null) {
                    foreach(Node child in root.children) {
                        if(child.element?.Name == path.FirstPart) {
                            if(path.Length == 1)
                                return child;

                            return Find(child, new FieldPath(path.ToList().Skip(1)));
                        }
                    }
                }

                return null;
            }

            private void BuildSchema(Node parent, List<Thrift.SchemaElement> schema, int count, ref int i) {
                parent.children = new List<Node>();
                for(int ic = 0; ic < count; ic++) {
                    Thrift.SchemaElement child = schema[i++];
                    var node = new Node { element = child, parent = parent };
                    parent.children.Add(node);
                    if(child.Num_children > 0) {
                        BuildSchema(node, schema, child.Num_children, ref i);
                    }
                }
            }
        }

#endregion
    }
}