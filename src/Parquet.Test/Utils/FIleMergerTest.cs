using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Utils;
using Xunit;

namespace Parquet.Test.Utils;

public class FIleMergerTest {

    [Fact]
    public async Task MergeFilesAsync_merges_three_files_and_preserves_data() {
        string tempDirectoryPath = Path.Combine(Path.GetTempPath(), "parquet-dotnet-file-merger-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDirectoryPath);

        try {
            DataField<int> id = new("id");
            ParquetSchema schema = new ParquetSchema(id);

            int[][] sourceValues = {
                new[] { 1, 2 },
                new[] { 3, 4 },
                new[] { 5, 6 }
            };

            List<FileInfo> inputFiles = new List<FileInfo>();
            for(int i = 0; i < sourceValues.Length; i++) {
                string filePath = Path.Combine(tempDirectoryPath, $"source-{i}.parquet");
                await WriteParquetFileAsync(filePath, schema, id, sourceValues[i]);
                inputFiles.Add(new FileInfo(filePath));
            }

            string mergedFilePath = Path.Combine(tempDirectoryPath, "merged.parquet");
            await using(FileStream mergedDestination = System.IO.File.Create(mergedFilePath)) {
                await using var merger = new FileMerger(inputFiles);
                await merger.MergeFilesAsync(mergedDestination);
            }

            List<int> mergedValues = new List<int>();
            await using FileStream mergedSource = System.IO.File.OpenRead(mergedFilePath);
            await using(ParquetReader reader = await ParquetReader.CreateAsync(mergedSource)) {
                Assert.Equal(3, reader.RowGroupCount);

                for(int rowGroupIndex = 0; rowGroupIndex < reader.RowGroupCount; rowGroupIndex++) {
                    using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
                    using RawColumnData<int> column = await rowGroupReader.ReadRawColumnDataAsync<int>(id);
                    mergedValues.AddRange(column.Values.ToArray());
                }
            }

            Assert.Equal(new[] { 1, 2, 3, 4, 5, 6 }, mergedValues);
        } finally {
            if(Directory.Exists(tempDirectoryPath)) {
                Directory.Delete(tempDirectoryPath, recursive: true);
            }
        }
    }

    [Fact]
    public async Task MergeRowGroups_merges_all_source_row_groups_into_single_row_group() {
        string tempDirectoryPath = Path.Combine(Path.GetTempPath(), "parquet-dotnet-file-merger-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDirectoryPath);

        try {
            DataField<int> id = new("id");
            ParquetSchema schema = new ParquetSchema(id);

            int[][] sourceValues = {
                new[] { 1, 2 },
                new[] { 3, 4 },
                new[] { 5, 6 }
            };

            List<FileInfo> inputFiles = new List<FileInfo>();
            for(int i = 0; i < sourceValues.Length; i++) {
                string filePath = Path.Combine(tempDirectoryPath, $"source-merge-rg-{i}.parquet");
                await WriteParquetFileAsync(filePath, schema, id, sourceValues[i]);
                inputFiles.Add(new FileInfo(filePath));
            }

            string mergedFilePath = Path.Combine(tempDirectoryPath, "merged-rowgroups.parquet");
            await using(FileStream mergedDestination = System.IO.File.Create(mergedFilePath)) {
                await using var merger = new FileMerger(inputFiles);
                await merger.MergeRowGroupsAsync(mergedDestination);
            }

            List<int> mergedValues = new List<int>();
            await using FileStream mergedSource = System.IO.File.OpenRead(mergedFilePath);
            await using(ParquetReader reader = await ParquetReader.CreateAsync(mergedSource)) {
                Assert.Equal(1, reader.RowGroupCount);

                using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
                Assert.Equal(6, rowGroupReader.RowCount);
                Assert.Equal(6, rowGroupReader.GetMetadata(id)?.MetaData?.NumValues);
                using RawColumnData<int> column = await rowGroupReader.ReadRawColumnDataAsync<int>(id);
                mergedValues.AddRange(column.Values.ToArray());
            }

            Assert.Equal(new[] { 1, 2, 3, 4, 5, 6 }, mergedValues);
        } finally {
            if(Directory.Exists(tempDirectoryPath)) {
                Directory.Delete(tempDirectoryPath, recursive: true);
            }
        }
    }

    [Fact]
    public async Task MergeRowGroupsAsync_with_custom_row_group_size_merges_successfully() {
        string tempDirectoryPath = Path.Combine(Path.GetTempPath(), "parquet-dotnet-file-merger-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDirectoryPath);

        try {
            DataField<int> id = new("id");
            ParquetSchema schema = new ParquetSchema(id);

            int[][] sourceValues = {
                new[] { 1, 2 },
                new[] { 3, 4 },
                new[] { 5, 6 }
            };

            List<FileInfo> inputFiles = new List<FileInfo>();
            for(int i = 0; i < sourceValues.Length; i++) {
                string filePath = Path.Combine(tempDirectoryPath, $"source-custom-rg-size-ok-{i}.parquet");
                await WriteParquetFileAsync(filePath, schema, id, sourceValues[i]);
                inputFiles.Add(new FileInfo(filePath));
            }

            string mergedFilePath = Path.Combine(tempDirectoryPath, "merged-custom-rg-size-ok.parquet");
            await using(FileStream mergedDestination = System.IO.File.Create(mergedFilePath)) {
                await using FileMerger merger = new FileMerger(inputFiles);
                await merger.MergeRowGroupsAsync(mergedDestination, rowGroupSize: 6);
            }

            await using FileStream mergedSource = System.IO.File.OpenRead(mergedFilePath);
            await using(ParquetReader reader = await ParquetReader.CreateAsync(mergedSource)) {
                Assert.Equal(1, reader.RowGroupCount);
                using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
                Assert.Equal(6, rowGroupReader.RowCount);
                Assert.Equal(6, rowGroupReader.GetMetadata(id)?.MetaData?.NumValues);
            }
        } finally {
            if(Directory.Exists(tempDirectoryPath)) {
                Directory.Delete(tempDirectoryPath, recursive: true);
            }
        }
    }

    [Fact]
    public async Task MergeRowGroupsAsync_with_too_small_custom_row_group_size_throws() {
        string tempDirectoryPath = Path.Combine(Path.GetTempPath(), "parquet-dotnet-file-merger-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDirectoryPath);

        try {
            DataField<int> id = new("id");
            ParquetSchema schema = new ParquetSchema(id);

            int[][] sourceValues = {
                new[] { 1, 2 },
                new[] { 3, 4 },
                new[] { 5, 6 }
            };

            List<FileInfo> inputFiles = new List<FileInfo>();
            for(int i = 0; i < sourceValues.Length; i++) {
                string filePath = Path.Combine(tempDirectoryPath, $"source-custom-rg-size-fail-{i}.parquet");
                await WriteParquetFileAsync(filePath, schema, id, sourceValues[i]);
                inputFiles.Add(new FileInfo(filePath));
            }

            string mergedFilePath = Path.Combine(tempDirectoryPath, "merged-custom-rg-size-fail.parquet");
            await using FileStream mergedDestination = System.IO.File.Create(mergedFilePath);
            await using FileMerger merger = new FileMerger(inputFiles);

            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => merger.MergeRowGroupsAsync(mergedDestination, rowGroupSize: 5));

            Assert.Contains("rowGroupSize", exception.Message);
        } finally {
            if(Directory.Exists(tempDirectoryPath)) {
                Directory.Delete(tempDirectoryPath, recursive: true);
            }
        }
    }

    private static async Task WriteParquetFileAsync(string filePath, ParquetSchema schema, DataField<int> field, int[] values) {
        await using FileStream stream = System.IO.File.Create(filePath);
        await using ParquetWriter writer = await ParquetWriter.CreateAsync(schema, stream);
        using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
        await rowGroup.WriteAsync<int>(field, values);
        rowGroup.CompleteValidate();
    }
}
