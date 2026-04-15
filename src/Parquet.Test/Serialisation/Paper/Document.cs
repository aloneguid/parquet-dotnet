using System;
using System.Buffers;
using System.Collections.Generic;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Test.Serialisation.Paper;

class Document {
    public long DocId { get; set; }

    public Links? Links { get; set; }

    public List<Name>? Name { get; set; }

    public static Document R1 => new() {
        DocId = 10,
        Links = new Links {
            Forward = new List<long> { 20, 40, 60 }
        },
        Name = new List<Name> {
                new Name {
                    Language = new List<Language> {
                        new Language {
                            Code = "en-us",
                            Country = "us"
                        },
                        new Language {
                            Code = "en"
                        }
                    },
                    Url = "http://A"
                },
                new Name {
                    Url = "http://B"
                },
                new Name {
                    Language = new List<Language> {
                        new Language {
                            Code = "en-gb",
                            Country = "gb"
                        }
                    }
                }
            }
    };

    public static Document R2 => new() {
        DocId = 20,
        Links = new Links {
            Backward = new List<long> { 10, 30 },
            Forward = new List<long> { 80 }
        },
        Name = new List<Name> {
                new Name {
                    Url = "http://C"
                }
            }
    };

    public static List<Document> Both => new List<Document> { R1, R2 };

    public static ParquetSchema RawColumnsSchema => new ParquetSchema(
                new DataField<long>("DocId"),
                new StructField("Links",
                    new ListField("Backward", new DataField<long>("element")),
                    new ListField("Forward", new DataField<long>("element"))),
                new ListField("Name",
                    new StructField("element",
                        new ListField("Language",
                            new StructField("element",
                                new DataField<string>("Code"),
                                new DataField<string>("Country"))),
                        new DataField<string>("Url")
                    )));

    public static object[] RawColumns {
        get {
            return new object[] {
                CreateRawColumnData(new long[] { 10, 20 }),

                // Links/Backwards
                CreateRawColumnData(
                    new long[] { 10, 30 },
                    new int[] { 1, 3, 3 },
                    new int[] { 0, 0, 1 }),

                // Links/Forwards
                CreateRawColumnData(
                    new long[] { 20, 40, 60, 80 },
                    new int[] { 3, 3, 3, 3 },
                    new int[] { 0, 1, 1, 0 }),

                // Code
                CreateRawColumnData(
                    new string[] { "en-us", "en", "en-gb" },
                    new int[] { 7, 7, 2, 7, 2 },
                    new int[] { 0, 2, 1, 1, 0 }),

                // Country
                CreateRawColumnData(
                    new string[] { "us", "gb" },
                    new int[] { 7, 6, 2, 7, 2 },
                    new int[] { 0, 2, 1, 1, 0 }),

                // Url
                CreateRawColumnData(
                    new string[] { "http://A", "http://B", "http://C" },
                    new int[] { 4, 4, 3, 4 },
                    new int[] { 0, 1, 1, 0 })
            };
        }
    }

    private static RawColumnData<T> CreateRawColumnData<T>(T[] values, int[]? definitionLevels = null, int[]? repetitionLevels = null)
        where T : struct {
        return new RawColumnData<T>(
            new ArrayMemoryOwner<T>(values),
            definitionLevels == null ? null : new ArrayMemoryOwner<int>(definitionLevels),
            repetitionLevels == null ? null : new ArrayMemoryOwner<int>(repetitionLevels));
    }

    private static RawColumnData<ReadOnlyMemory<char>> CreateRawColumnData(string[] values, int[]? definitionLevels = null, int[]? repetitionLevels = null) {
        ReadOnlyMemory<char>[] chars = new ReadOnlyMemory<char>[values.Length];
        for(int i = 0; i < values.Length; i++) {
            chars[i] = values[i].AsMemory();
        }

        return CreateRawColumnData(chars, definitionLevels, repetitionLevels);
    }

    private sealed class ArrayMemoryOwner<T> : IMemoryOwner<T> {
        private T[]? _buffer;

        public ArrayMemoryOwner(T[] buffer) {
            _buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
        }

        public Memory<T> Memory => _buffer;

        public void Dispose() {
            _buffer = null;
        }
    }
}
