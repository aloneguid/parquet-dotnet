using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Schema;
using Parquet.Data;
using System.Linq;

namespace Parquet.Utils {

    /// <summary>
    /// Concatenates multiple files into a single file. The resulting file will contain all the data and row groups from the original files.
    /// Schemas in all the files must match.
    /// </summary>
    public class FileMerger {

        private const string ParquetFileExtension = ".parquet";

        private readonly List<FileInfo> _files = new();

        /// <summary>
        /// Specifies the directory containing the files to concatenate.
        /// All the files with ".parquet" extension will be concatenated recursively.
        /// </summary>
        /// <param name="directory"></param>
        public FileMerger(DirectoryInfo directory) {

            foreach(FileInfo fi in directory.EnumerateFiles("*" + ParquetFileExtension, SearchOption.AllDirectories)) {
                _files.Add(fi);
            }
        }

        /// <summary>
        /// Specifies the files to concatenate. All the files must have the same schema due to parquet file format restrictions.
        /// </summary>
        /// <param name="files"></param>
        public FileMerger(IEnumerable<FileInfo> files) {
            _files.AddRange(files);
        }

        /// <summary>
        /// All the input files to concatenate.
        /// </summary>
        public IReadOnlyCollection<FileInfo> InputFiles => _files;

        /// <summary>
        /// Merges all the files into a single file by copying row groups from each file into the resulting file.
        /// The resulting file will end up having as many row groups as the sum of row groups in all the files.
        /// </summary>
        public async Task MergeFilesAsync(Stream destination,
            ParquetOptions? options = null,
            CancellationToken cancellationToken = default) {

            if(_files.Count == 0) {
                throw new InvalidOperationException("No files to merge");
            }

            if(!destination.CanWrite) {
                throw new ArgumentException("Destination stream must be writable", nameof(destination));
            }

            // the first file will be taken as is
            using(Stream src = System.IO.File.OpenRead(_files[0].FullName)) {
#if NETSTANDARD2_0
                await src.CopyToAsync(destination);
#else
                await src.CopyToAsync(destination, cancellationToken);
#endif
            }

            // get the schema from the first file, it will be used to validate the rest of the files
            ParquetSchema schema = await ParquetReader.ReadSchemaAsync(_files[0].FullName);

            // create writer for the destination file
            using ParquetWriter destWriter = await ParquetWriter.CreateAsync(schema, destination, options, true, cancellationToken);

            // the rest of the files will be appended
            for(int i = 1; i < _files.Count; i++) {
                using ParquetReader pr = await ParquetReader.CreateAsync(_files[i].FullName, options, cancellationToken);

                for(int ig = 0; ig < pr.RowGroupCount; ig++) {
                    using ParquetRowGroupReader rrg = pr.OpenRowGroupReader(ig);
                    using ParquetRowGroupWriter wrg = destWriter.CreateRowGroup();
                    
                    // read all the columns in the row group and write to the destination row group
                    foreach(DataField dataField in schema.DataFields) {
                        DataColumn dataColumn = await rrg.ReadColumnAsync(dataField, cancellationToken);
                        await wrg.WriteColumnAsync(dataColumn, cancellationToken);
                    }
                }
            }
        }

        /// <summary>
        /// Merges all the row groups in the files into a single row group in the resulting file.
        /// If source files have more than one row group, they will be stil merged into one
        /// row group in the destination file, therefore you can use this method even on a single file if you
        /// want to just merge all the row groups into one.
        /// </summary>
        /// <param name="destination"></param>
        /// <param name="options"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task MergeRowGroups(Stream destination,
            ParquetOptions? options = null,
            CancellationToken cancellationToken = default) {

            if(_files.Count == 0) {
                throw new InvalidOperationException("No files to merge");
            }

            if(!destination.CanWrite) {
                throw new ArgumentException("Destination stream must be writable", nameof(destination));
            }

            // get the schema from the first file, it will be used to validate the rest of the files
            ParquetSchema schema = await ParquetReader.ReadSchemaAsync(_files[0].FullName);

            // create writer for the destination file
            using ParquetWriter destWriter = await ParquetWriter.CreateAsync(schema, destination, options, false, cancellationToken);

            // We will open all of the files to utilise random access. They need to be opened sequentially one by one,
            // to avoid an error of not disposing successfully opened files in case not all of them are valid.

            var readers = new List<ParquetReader>(_files.Count);

            try {
                foreach(FileInfo fi in _files) {
                    ParquetReader reader = await ParquetReader.CreateAsync(fi.FullName, options, cancellationToken);
                    readers.Add(reader);
                }

                // merge logic

                // create a row group writer for the destination file
                using ParquetRowGroupWriter wrg = destWriter.CreateRowGroup();

                // as merging is memory intenstive, we will read and write column by column
                // this way we will not load entire row group into memory

                foreach(DataField dataField in schema.DataFields) {

                    var dataColumns = new List<DataColumn>();

                    // read all data columns
                    foreach(ParquetReader reader in readers) {
                        for(int ig = 0; ig < reader.RowGroupCount; ig++) {
                            using ParquetRowGroupReader rrg = reader.OpenRowGroupReader(ig);

                            DataColumn dataColumn = await rrg.ReadColumnAsync(dataField, cancellationToken);
                            dataColumns.Add(dataColumn);
                        }
                    }

                    // merge and write to the destination
                    DataColumn mergedColumn = DataColumn.Concat(dataColumns);
                    await wrg.WriteColumnAsync(mergedColumn, cancellationToken);
                }

            } finally {
                foreach(ParquetReader reader in readers) {
                    reader.Dispose();
                }
            }
        }
    }
}
