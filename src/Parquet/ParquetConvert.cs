using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Extensions;
using Parquet.Schema;
using Parquet.Serialization;

namespace Parquet {
    /// <summary>
    /// High-level object oriented API for Apache Parquet
    /// </summary>
    [Obsolete(Globals.ParquetConvertObsolete)]
    public static class ParquetConvert {
        /// <summary>
        /// Serialises a collection of classes into a Parquet stream
        /// </summary>
        /// <typeparam name="T">Class type</typeparam>
        /// <param name="objectInstances">Collection of classes</param>
        /// <param name="destination">Destination stream</param>
        /// <param name="schema">Optional schema to use. When not specified the class schema will be discovered and everything possible will be
        /// written to the stream. If you want to write only a subset of class properties please specify the schema yourself.
        /// </param>
        /// <param name="compressionMethod"><see cref="CompressionMethod"/></param>
        /// <param name="rowGroupSize"></param>
        /// <param name="append"></param>
        /// <returns></returns>
        public static async Task<ParquetSchema> SerializeAsync<T>(IEnumerable<T> objectInstances, Stream destination,
            ParquetSchema? schema = null,
            CompressionMethod compressionMethod = CompressionMethod.Snappy,
            int rowGroupSize = 5000,
            bool append = false) {

            if(objectInstances == null)
                throw new ArgumentNullException(nameof(objectInstances));
            if(destination == null)
                throw new ArgumentNullException(nameof(destination));
            if(!destination.CanWrite)
                throw new ArgumentException("stream must be writeable", nameof(destination));

            //if schema is not passed reflect it
            if(schema == null) {
                schema = typeof(T).GetParquetSchema(false);
            }

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, destination, append: append)) {
                writer.CompressionMethod = compressionMethod;

                DataField[] dataFields = schema.GetDataFields();

                foreach(IEnumerable<T> batch in objectInstances.Batch(rowGroupSize)) {
                    var bridge = new ClrBridge(typeof(T));
                    T[] batchArray = batch.ToArray();

                    DataColumn[] columns = dataFields
                        .Select(df => bridge.BuildColumn(df, batchArray, batchArray.Length))
                        .ToArray();

                    using(ParquetRowGroupWriter groupWriter = writer.CreateRowGroup()) {
                        foreach(DataColumn dataColumn in columns) {
                            await groupWriter.WriteColumnAsync(dataColumn);
                        }
                    }
                }
            }

            return schema;
        }

        /// <summary>
        /// Serialises a collection of classes into a Parquet file
        /// </summary>
        /// <typeparam name="T">Class type</typeparam>
        /// <param name="objectInstances">Collection of classes</param>
        /// <param name="filePath">Destination file path</param>
        /// <param name="schema">Optional schema to use. When not specified the class schema will be discovered and everything possible will be
        /// written to the stream. If you want to write only a subset of class properties please specify the schema yourself.
        /// </param>
        /// <param name="compressionMethod"><see cref="CompressionMethod"/></param>
        /// <param name="rowGroupSize"></param>
        /// <param name="append"></param>
        /// <returns></returns>
        public static async Task<ParquetSchema> SerializeAsync<T>(IEnumerable<T> objectInstances, string filePath,
            ParquetSchema? schema = null,
            CompressionMethod compressionMethod = CompressionMethod.Snappy,
            int rowGroupSize = 5000,
            bool append = false) {
            using(Stream destination = System.IO.File.Open(filePath, FileMode.OpenOrCreate)) {
                return await SerializeAsync(objectInstances, destination, schema, compressionMethod, rowGroupSize,
                    append);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public static async Task<T[]> DeserializeAsync<T>(Stream input,
            int rowGroupIndex = -1,
            ParquetSchema? fileSchema = null,
            ParquetOptions? options = null,
            CancellationToken cancellationToken = default)
            where T : new() {
            var result = new List<T>();
            using(ParquetReader reader = await ParquetReader.CreateAsync(input, options, true, cancellationToken)) {
                if(fileSchema == null) {
                    fileSchema = typeof(T).GetParquetSchema(true);
                }

                DataField[] dataFields = fileSchema.GetDataFields();

                if(rowGroupIndex == -1) //Means read all row groups.
                {
                    for(int i = 0; i < reader.RowGroupCount; i++) {
                        T[] currentRowGroupRecords = await ReadAndDeserializeByRowGroupAsync<T>(i, reader, dataFields);
                        result.AddRange(currentRowGroupRecords);
                    }
                }
                else //read specific rowgroup.
                {
                    T[] currentRowGroupRecords =
                        await ReadAndDeserializeByRowGroupAsync<T>(rowGroupIndex, reader, dataFields);
                    result.AddRange(currentRowGroupRecords);
                }
            }

            return result.ToArray();
        }

        /// <summary>
        /// 
        /// </summary>
        public static async Task<T[]> DeserializeAsync<T>(string fileName,
            int rowGroupIndex = -1,
            ParquetSchema? fileSchema = null,
            ParquetOptions? options = null,
            CancellationToken cancellationToken = default)
            where T : new() {
            using Stream fs = System.IO.File.OpenRead(fileName);
            return await DeserializeAsync<T>(fs, rowGroupIndex, fileSchema, options, cancellationToken);
        }

        private static async Task<T[]> ReadAndDeserializeByRowGroupAsync<T>(int rowGroupIndex, ParquetReader reader,
            DataField[] dataFields) where T : new() {
            var bridge = new ClrBridge(typeof(T));

            using(ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(rowGroupIndex)) {
                DataColumn[] groupColumns = await dataFields
                    .Select(df => groupReader.ReadColumnAsync(df))
                    .SequentialWhenAll();

                T[] rb = new T[groupReader.RowCount];
                for(int ie = 0; ie < rb.Length; ie++) {
                    rb[ie] = new T();
                }

                for(int ic = 0; ic < groupColumns.Length; ic++) {
                    bridge.AssignColumn(groupColumns[ic], rb);
                }

                return rb;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public static async Task<IEnumerable<T[]>> DeserializeGroupsAsync<T>(Stream input) where T : new() {
            var bridge = new ClrBridge(typeof(T));
            var r = new List<T[]>();

            using(ParquetReader reader = await ParquetReader.CreateAsync(input)) {
                ParquetSchema? fileSchema = reader.Schema;
                if(fileSchema== null) {
                    throw new InvalidOperationException("schema not defined");
                }
                DataField[] dataFields = fileSchema.GetDataFields();

                for(int i = 0; i < reader.RowGroupCount; i++) {
                    using(ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(i)) {

                        DataColumn[] groupColumns = await dataFields
                            .Select(df => groupReader.ReadColumnAsync(df))
                            .SequentialWhenAll();

                        T[] rb = new T[groupReader.RowCount];
                        for(int ie = 0; ie < rb.Length; ie++) {
                            rb[ie] = new T();
                        }

                        for(int ic = 0; ic < groupColumns.Length; ic++) {
                            bridge.AssignColumn(groupColumns[ic], rb);
                        }

                        r.Add(rb);
                    }
                }
            }

            return r;
        }
    }
}
