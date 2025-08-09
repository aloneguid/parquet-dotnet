using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Encodings;
using Parquet.Schema;
using Parquet.Meta;
using Parquet.Meta.Proto;

namespace Parquet.File {
    class ThriftFooter {
        private readonly FileMetaData _fileMeta;
        private readonly ThriftSchemaTree _tree;

        internal static ThriftFooter Empty => new();

        internal ThriftFooter() {
            _fileMeta = new FileMetaData();
            _tree= new ThriftSchemaTree();
        }

        public ThriftFooter(FileMetaData fileMeta) {
            _fileMeta = fileMeta ?? throw new ArgumentNullException(nameof(fileMeta));
            _tree = new ThriftSchemaTree(_fileMeta.Schema);
        }

        internal static ParquetSchema Parse(params SchemaElement[] elements) {

            var slst = new List<SchemaElement> {
                new SchemaElement { Name = "root", NumChildren = 1 },
            };
            slst.AddRange(elements);

            return new ThriftFooter(new FileMetaData {
                Schema = slst
            }).CreateModelSchema(new ParquetOptions());
        }

        public ThriftFooter(ParquetSchema schema, long totalRowCount) {
            if(schema == null) {
                throw new ArgumentNullException(nameof(schema));
            }

            _fileMeta = CreateThriftSchema(schema);
            _fileMeta.NumRows = totalRowCount;


            // Looks like Spark is sensitive about this format. See https://github.com/aloneguid/parquet-dotnet/issues/261
#if DEBUG
            _fileMeta.CreatedBy = "Parquet.Net version LocalDev (build Local)";
#else
            _fileMeta.CreatedBy = $"Parquet.Net version {Globals.Version} (build {Globals.GithubSha})";
#endif
            _tree = new ThriftSchemaTree(_fileMeta.Schema);
        }

        public Dictionary<string, string> CustomMetadata {
            set {
                _fileMeta.KeyValueMetadata = null;
                if(value == null || value.Count == 0)
                    return;

                _fileMeta.KeyValueMetadata = value
                   .Select(kvp => new KeyValue{ Key = kvp.Key, Value = kvp.Value })
                   .ToList();
            }
            get {
                if(_fileMeta.KeyValueMetadata == null || _fileMeta.KeyValueMetadata.Count == 0)
                    return new Dictionary<string, string>();

                return _fileMeta.KeyValueMetadata.ToDictionary(kv => kv.Key, kv => kv.Value!);
            }
        }

        public void Add(long totalRowCount) {
            _fileMeta.NumRows += totalRowCount;
        }

        public async Task<long> WriteAsync(Stream s, CancellationToken cancellationToken = default) {
            using var ms = new MemoryStream();
            _fileMeta.Write(new ThriftCompactProtocolWriter(ms));
            ms.Position = 0;
            await ms.CopyToAsync(s);
            return ms.Length;
        }

        public long Write(Stream s) {
            using var ms = new MemoryStream();
            _fileMeta.Write(new ThriftCompactProtocolWriter(ms));
            ms.Position = 0;
            ms.CopyTo(s);
            return ms.Length;
        }

        public SchemaElement? GetSchemaElement(ColumnChunk columnChunk) {
            if(columnChunk == null) {
                throw new ArgumentNullException(nameof(columnChunk));
            }

            var findPath = new FieldPath(columnChunk.MetaData!.PathInSchema);
            return _tree.Find(findPath)?.element;
        }

        public FieldPath GetPath(SchemaElement schemaElement) {
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

        public SchemaElement[] GetWriteableSchema() {
            return _fileMeta.Schema.Where(tse => tse.Type != null).ToArray();
        }

        public RowGroup AddRowGroup() {
            var rg = new RowGroup();
            _fileMeta.RowGroups ??= new List<RowGroup>();
            _fileMeta.RowGroups.Add(rg);
            return rg;
        }

        public ColumnChunk CreateColumnChunk(CompressionMethod compression, System.IO.Stream output,
            Parquet.Meta.Type columnType, FieldPath path, int valuesCount,
            Dictionary<string, string>? keyValueMetadata) {
            CompressionCodec codec = (CompressionCodec)(int)compression;

            var chunk = new ColumnChunk();
            long startPos = output.Position;
            chunk.FileOffset = startPos;
            chunk.MetaData = new ColumnMetaData();
            chunk.MetaData.NumValues = valuesCount;
            chunk.MetaData.Type = columnType;
            chunk.MetaData.Codec = codec;
            chunk.MetaData.DataPageOffset = startPos;
            chunk.MetaData.Encodings = new List<Encoding> {
                Encoding.RLE,
                Encoding.BIT_PACKED,
                Encoding.PLAIN
            };
            chunk.MetaData!.PathInSchema = path.ToList();
            chunk.MetaData!.Statistics = new Statistics();
            if(keyValueMetadata != null && keyValueMetadata.Count > 0) {
                chunk.MetaData.KeyValueMetadata = keyValueMetadata
                    .Select(kv => new KeyValue { Key = kv.Key, Value = kv.Value })
                    .ToList();
            }

            return chunk;
        }

        public PageHeader CreateDataPage(int valueCount, bool isDictionary, bool isDeltaEncodable) => 
            new PageHeader {
                Type = PageType.DATA_PAGE,
                DataPageHeader = new DataPageHeader {
                    Encoding = isDictionary
                        ? Encoding.PLAIN_DICTIONARY
                        : isDeltaEncodable ? Encoding.DELTA_BINARY_PACKED : Encoding.PLAIN,
                    DefinitionLevelEncoding = Encoding.RLE,
                    RepetitionLevelEncoding = Encoding.RLE,
                    NumValues = valueCount,
                    Statistics = new Statistics()
                }
            };

        public PageHeader CreateDictionaryPage(int numValues) {
            var ph = new PageHeader { 
                Type = PageType.DICTIONARY_PAGE,
                DictionaryPageHeader = new DictionaryPageHeader {
                    Encoding = Encoding.PLAIN_DICTIONARY,
                    NumValues = numValues
                }};
            return ph;
        }

#region [ Conversion to Model Schema ]

        public ParquetSchema CreateModelSchema(ParquetOptions formatOptions) {
            int si = 0;
            SchemaElement tse = _fileMeta.Schema[si++];
            var container = new List<Field>();

            CreateModelSchema(null, container, tse.NumChildren ?? 0, ref si, formatOptions);

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

        public FileMetaData CreateThriftSchema(ParquetSchema schema) {
            var meta = new FileMetaData();
            meta.Version = 1;
            meta.Schema = new List<SchemaElement>();
            meta.RowGroups = new List<RowGroup>();

            SchemaElement root = ThriftFooter.AddRoot(meta.Schema);
            foreach(Field se in schema.Fields) {
                SchemaEncoder.Encode(se, root, meta.Schema);
            }

            return meta;
        }


        private static SchemaElement AddRoot(IList<SchemaElement> container) {
            var root = new SchemaElement { Name = "root" };
            container.Add(root);
            return root;
        }

#endregion

#region [ Helpers ]

        class ThriftSchemaTree {
            readonly Dictionary<SchemaElement, Node?> _memoizedFindResults = 
                new Dictionary<SchemaElement, Node?>(new ReferenceEqualityComparer<SchemaElement>());

            public class Node {
                public SchemaElement? element;
                public List<Node>? children;
                public Node? parent;
            }

            public Node root;

            internal ThriftSchemaTree() {
                root = new Node();
            }

            public ThriftSchemaTree(List<SchemaElement> schema) {
                root = new Node { element = schema[0] };
                int i = 1;

                BuildSchema(root, schema, root.element.NumChildren ?? 0, ref i);
            }

            public Node? Find(SchemaElement tse) {
                if(_memoizedFindResults.TryGetValue(tse, out Node? node)) {
                    return node;
                }
                node = Find(root, tse);
                _memoizedFindResults.Add(tse, node);
                return node;
            }

            private Node? Find(Node root, SchemaElement tse) {
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

            private void BuildSchema(Node parent, List<SchemaElement> schema, int count, ref int i) {
                parent.children = new List<Node>();
                for(int ic = 0; ic < count; ic++) {
                    SchemaElement child = schema[i++];
                    var node = new Node { element = child, parent = parent };
                    parent.children.Add(node);
                    if(child.NumChildren > 0) {
                        BuildSchema(node, schema, child.NumChildren ?? 0, ref i);
                    }
                }
            }
        }

#endregion
    }
}