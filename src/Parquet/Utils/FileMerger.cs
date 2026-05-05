using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CommunityToolkit.HighPerformance.Buffers;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Utils;


/// <summary>
/// Concatenates multiple files into a single file. The resulting file will contain all the data and row groups from the
/// original files. Schemas in all the files must match.
/// </summary>
public class FileMerger : IAsyncDisposable {

    private const string ParquetFileExtension = ".parquet";
    private const int DefaultMergeRowGroupSize = 1_000_000;

    private readonly List<FileInfo> _inputFiles = new();
    private readonly List<Stream> _inputStreams = new();

    /// <summary>
    /// Specifies the directory containing the files to concatenate. All the files with ".parquet" extension will be
    /// concatenated recursively.
    /// </summary>
    /// <param name="directory"></param>
    public FileMerger(DirectoryInfo directory) {

        foreach(FileInfo fi in directory.EnumerateFiles("*" + ParquetFileExtension, SearchOption.AllDirectories)) {
            _inputFiles.Add(fi);
            _inputStreams.Add(fi.OpenRead());
        }
    }

    /// <summary>
    /// Specifies the files to concatenate. All the files must have the same schema due to parquet file format
    /// restrictions.
    /// </summary>
    /// <param name="files"></param>
    public FileMerger(IEnumerable<FileInfo> files) {
        _inputFiles.AddRange(files);
        _inputStreams.AddRange(files.Select(f => f.OpenRead()));
    }

    /// <summary>
    /// All the input streams to concatenate.
    /// </summary>
    public IReadOnlyCollection<Stream> InputStreams => _inputStreams;

    /// <summary>
    /// All the input files to concatenate, if this instance was created using <see cref="DirectoryInfo"/> or
    /// <see cref="FileInfo"/> constructor.
    /// </summary>
    public IReadOnlyCollection<FileInfo> InputFiles => _inputFiles;

    /// <summary>
    /// Merges all the files into a single file by copying row groups from each file into the resulting file. The
    /// resulting file will end up having as many row groups as the sum of row groups in all the files. The result looks
    /// almost like you have done something like `cat file1.parquet file2.parquet > merged.parquet` in the command line,
    /// but with parquet files. This is the most efficient way to merge files, as it does not require reading and
    /// writing data, but just copying bytes from source files to the destination file. However, it requires that all
    /// the files have exactly the same schema and compatible row group sizes, otherwise the resulting file will be
    /// invalid.
    /// </summary>
    public async Task MergeFilesAsync(Stream destination,
        ParquetOptions? options = null,
        CancellationToken cancellationToken = default,
        Dictionary<string, string>? metadata = null) {

        if(_inputStreams.Count == 0) {
            throw new InvalidOperationException("No files to merge");
        }

        if(!destination.CanWrite) {
            throw new ArgumentException("Destination stream must be writable", nameof(destination));
        }

        // the first file will be taken as is
        Stream src = _inputStreams[0];
        await src.CopyToAsync(destination, cancellationToken);

        // get the schema from the first file, it will be used to validate the rest of the files
        ParquetSchema schema = await ParquetReader.ReadSchemaAsync(src);

        // create writer for the destination file
        await using ParquetWriter destWriter = await ParquetWriter.CreateAsync(schema, destination, options, true, cancellationToken);

        if(metadata != null) {
            destWriter.CustomMetadata = metadata;
        }

        // the rest of the files will be appended
        for(int i = 1; i < _inputStreams.Count; i++) {
            await using ParquetReader pr = await ParquetReader.CreateAsync(_inputStreams[i], options, cancellationToken: cancellationToken);

            for(int ig = 0; ig < pr.RowGroupCount; ig++) {
                using ParquetRowGroupReader rrg = pr.OpenRowGroupReader(ig);
                using ParquetRowGroupWriter wrg = destWriter.CreateRowGroup();

                // read all the columns in the row group and write to the destination row group
                foreach(DataField dataField in schema.DataFields) {
                    using RawColumnData rcd = await rrg.ReadRawColumnDataBaseAsync(dataField, cancellationToken);
                    await wrg.WriteAsync(dataField, rcd, cancellationToken);
                }
            }
        }
    }

    /// <summary>
    /// Merges all the row groups in the files into a single row group in the resulting file. If source files have more
    /// than one row group, they will be stil merged into one row group in the destination file, therefore you can use
    /// this method even on a single file if you want to just merge all the row groups into one.
    /// </summary>
    /// <param name="destination"></param>
    /// <param name="options"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="metadata"></param>
    /// <param name="rowGroupSize">Maximum number of elements allowed per merged field buffer.</param>
    /// <returns></returns>
    public Task MergeRowGroups(Stream destination,
        ParquetOptions? options = null,
        CancellationToken cancellationToken = default,
        Dictionary<string, string>? metadata = null,
        int rowGroupSize = DefaultMergeRowGroupSize) {
        return MergeRowGroupsAsync(destination, options, cancellationToken, metadata, rowGroupSize);
    }

    /// <summary>
    /// Merges all the row groups in the files into a single row group in the resulting file. If source files have more
    /// than one row group, they will be stil merged into one row group in the destination file, therefore you can use
    /// this method even on a single file if you want to just merge all the row groups into one.
    /// </summary>
    /// <param name="destination"></param>
    /// <param name="options"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="metadata"></param>
    /// <param name="rowGroupSize">Maximum number of elements allowed per merged field buffer.</param>
    /// <returns></returns>
    public async Task MergeRowGroupsAsync(Stream destination,
        ParquetOptions? options = null,
        CancellationToken cancellationToken = default,
        Dictionary<string, string>? metadata = null,
        int rowGroupSize = DefaultMergeRowGroupSize) {

        if(_inputStreams.Count == 0) {
            throw new InvalidOperationException("No files to merge");
        }

        if(!destination.CanWrite) {
            throw new ArgumentException("Destination stream must be writable", nameof(destination));
        }

        if(rowGroupSize <= 0) {
            throw new ArgumentOutOfRangeException(nameof(rowGroupSize), "Row group size must be greater than zero.");
        }

        foreach(Stream inputStream in _inputStreams) {
            if(!inputStream.CanSeek) {
                throw new IOException("Input streams must be seekable for row group merge operations.");
            }

            inputStream.Position = 0;
        }

        List<ParquetReader> readers = new List<ParquetReader>(_inputStreams.Count);

        try {
            foreach(Stream s in _inputStreams) {
                ParquetReader reader = await ParquetReader.CreateAsync(s, options, cancellationToken: cancellationToken);
                readers.Add(reader);
            }

            if(readers.Sum(r => r.RowGroupCount) == 0) {
                throw new InvalidOperationException("Input files do not contain any row groups to merge.");
            }

            ParquetSchema schema = readers[0].Schema;
            if(schema.DataFields.Length == 0) {
                throw new InvalidOperationException("Input schema does not contain data fields to merge.");
            }

            // create writer for the destination file
            await using ParquetWriter destWriter = await ParquetWriter.CreateAsync(schema, destination, options, false, cancellationToken);

            if(metadata != null) {
                destWriter.CustomMetadata = metadata;
            }

            using ParquetRowGroupWriter wrg = destWriter.CreateRowGroup();

            foreach(DataField dataField in schema.DataFields) {
                await MergeFieldAsync(dataField, readers, wrg, rowGroupSize, cancellationToken);
            }

            wrg.CompleteValidate();
        } finally {
            foreach(ParquetReader reader in readers) {
                await reader.DisposeAsync();
            }
        }
    }

    private static async Task MergeFieldAsync(DataField dataField,
        IReadOnlyList<ParquetReader> readers,
        ParquetRowGroupWriter writer,
        int rowGroupSize,
        CancellationToken cancellationToken) {

        Type elementType = dataField.ClrValueType;
        System.Reflection.MethodInfo method = typeof(FileMerger)
            .GetMethod(nameof(MergeFieldTypedAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
            ?? throw new InvalidOperationException($"can't find {nameof(MergeFieldTypedAsync)} method on {nameof(FileMerger)}");

        System.Reflection.MethodInfo genericMethod = method.MakeGenericMethod(elementType);
        Task task = (Task?)genericMethod.Invoke(null, new object[] { dataField, readers, writer, rowGroupSize, cancellationToken })
            ?? throw new InvalidOperationException($"failed to invoke {nameof(MergeFieldTypedAsync)} for type {elementType}");

        await task;
    }

    private static async Task MergeFieldTypedAsync<T>(DataField dataField,
        IReadOnlyList<ParquetReader> readers,
        ParquetRowGroupWriter writer,
        int rowGroupSize,
        CancellationToken cancellationToken) where T : struct {

        List<RawColumnData<T>> rawColumns = new List<RawColumnData<T>>();

        try {
            foreach(ParquetReader reader in readers) {
                for(int ig = 0; ig < reader.RowGroupCount; ig++) {
                    using ParquetRowGroupReader rrg = reader.OpenRowGroupReader(ig);
                    RawColumnData<T> rawData = await rrg.ReadRawColumnDataAsync<T>(dataField, cancellationToken);
                    rawColumns.Add(rawData);
                }
            }

            int valueCount = rawColumns.Sum(c => c.ValuesMemory.Length);
            if(readers.Sum(r => r.RowGroupCount) > 0 && valueCount == 0) {
                throw new InvalidOperationException($"No values were read for field '{dataField.Path}' while source row groups are present.");
            }

            if(valueCount > rowGroupSize) {
                throw new InvalidOperationException(
                    $"Field '{dataField.Path}' contains {valueCount} values which exceeds configured rowGroupSize {rowGroupSize}. Increase rowGroupSize to continue.");
            }

            using MemoryOwner<T> mergedValues = MemoryOwner<T>.Allocate(valueCount);
            int valuesOffset = 0;

            foreach(RawColumnData<T> column in rawColumns) {
                column.ValuesMemory.Span.CopyTo(mergedValues.Memory.Span.Slice(valuesOffset));
                valuesOffset += column.ValuesMemory.Length;
            }

            MemoryOwner<int>? mergedRepetitionLevels = null;
            if(dataField.MaxRepetitionLevel > 0) {
                int repetitionCount = rawColumns.Sum(c => c.RepetitionLevelsMemoryOrNull?.Length ?? 0);
                mergedRepetitionLevels = MemoryOwner<int>.Allocate(repetitionCount);
                int repetitionOffset = 0;

                foreach(RawColumnData<T> column in rawColumns) {
                    ReadOnlyMemory<int> repetitionLevels = column.RepetitionLevelsMemoryOrNull
                        ?? throw new InvalidOperationException($"repetition levels are missing for field '{dataField.Path}'");
                    repetitionLevels.Span.CopyTo(mergedRepetitionLevels.Memory.Span.Slice(repetitionOffset));
                    repetitionOffset += repetitionLevels.Length;
                }
            }

            try {
                if(dataField.MaxDefinitionLevel > 0) {
                    int definitionCount = rawColumns.Sum(c => c.DefinitionLevelsMemoryOrNull?.Length ?? 0);
                    using MemoryOwner<int> mergedDefinitionLevels = MemoryOwner<int>.Allocate(definitionCount);
                    int definitionOffset = 0;

                    foreach(RawColumnData<T> column in rawColumns) {
                        ReadOnlyMemory<int> definitionLevels = column.DefinitionLevelsMemoryOrNull
                            ?? throw new InvalidOperationException($"definition levels are missing for field '{dataField.Path}'");
                        definitionLevels.Span.CopyTo(mergedDefinitionLevels.Memory.Span.Slice(definitionOffset));
                        definitionOffset += definitionLevels.Length;
                    }

                    using MemoryOwner<T?> nullableValues = MemoryOwner<T?>.Allocate(mergedDefinitionLevels.Length);
                    int valueIndex = 0;
                    for(int i = 0; i < mergedDefinitionLevels.Length; i++) {
                        bool isNull = mergedDefinitionLevels.Span[i] < dataField.MaxDefinitionLevel;
                        nullableValues.Span[i] = isNull ? null : mergedValues.Span[valueIndex++];
                    }

                    await writer.WriteAsync<T>(dataField,
                        nullableValues.Memory,
                        repetitionLevels: mergedRepetitionLevels?.Memory,
                        customMetadata: null,
                        cancellationToken: cancellationToken);
                } else {
                    await writer.WriteAsync<T>(dataField,
                        mergedValues.Memory,
                        repetitionLevels: mergedRepetitionLevels?.Memory,
                        customMetadata: null,
                        cancellationToken: cancellationToken);
                }
            } finally {
                mergedRepetitionLevels?.Dispose();
            }
        } finally {
            foreach(RawColumnData<T> column in rawColumns) {
                column.Dispose();
            }
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() {
        foreach(Stream stream in _inputStreams) {
            await stream.DisposeAsync();
        }
    }
}
