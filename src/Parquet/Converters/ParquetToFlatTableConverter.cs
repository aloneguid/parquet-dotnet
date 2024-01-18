﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Schema;
using Parquet.Serialization;

namespace Parquet.Converters {

    /// <summary>
    /// Converts Parquet to a flat table. This class needs to be inherited to inject a specific
    /// target format implementation.
    /// </summary>
    public abstract class ParquetToFlatTableConverter : IDisposable {
        private readonly Stream _parquetInputStream;
        private readonly ParquetOptions? _options;

        /// <summary>
        /// Invoked when a file is fully loaded. First argument is the number of rows in the file.
        /// </summary>
        public event Action<long>? OnFileOpened;

        /// <summary>
        /// Invoked when a row is converted. First argument is the row number, second argument is the total number of rows.
        /// </summary>
        public event Action<long, long>? OnRowConverted;

        /// <summary>
        /// Constructs a new instance of the converter
        /// </summary>
        /// <param name="parquetInputStream"></param>
        /// <param name="options"></param>
        protected ParquetToFlatTableConverter(Stream parquetInputStream, ParquetOptions? options = null) {
            _parquetInputStream = parquetInputStream;
            _options = options;
        }

        /// <summary>
        /// Gets the total number of rows in the Parquet file
        /// </summary>
        public long TotalRows { get; private set; } 

        /// <summary>
        /// Gets the number of rows converted so far
        /// </summary>
        public long ConvertedRows { get; private set; }

        /// <summary>
        /// Converts Parquet to a flat table
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task ConvertAsync(CancellationToken cancellationToken = default) {
            ParquetSerializer.UntypedResult parquetData = await ParquetSerializer.DeserializeAsync(
                _parquetInputStream, _options, cancellationToken);

            OnFileOpened?.Invoke(parquetData.Data.Count);

            TotalRows = parquetData.Data.Count;
            ConvertedRows = 0;

            await WriteHeaderAsync(parquetData.Schema, cancellationToken);

            foreach(Dictionary<string, object> row in parquetData.Data) {
                await NewRow();

                foreach(Field f in parquetData.Schema.Fields) {
                    if(f.SchemaType == SchemaType.Data) {
                        DataField df = (DataField)f;
                        row.TryGetValue(df.Name, out object? value);
                        await WriteCellAsync(df, value, cancellationToken);
                    }
                }

                ConvertedRows++;
                OnRowConverted?.Invoke(ConvertedRows, TotalRows);
            }
        }

        /// <summary>
        /// Invoked when the header should be written
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        protected abstract Task WriteHeaderAsync(ParquetSchema schema, CancellationToken cancellationToken = default);

        /// <summary>
        /// Invoked when a new row should be written
        /// </summary>
        /// <returns></returns>
        protected abstract Task NewRow();

        /// <summary>
        /// Invoked when a cell should be written
        /// </summary>
        /// <param name="df"></param>
        /// <param name="value"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        protected abstract Task WriteCellAsync(DataField df, object? value, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// 
        /// </summary>
        public virtual void Dispose() {

        }
    }
}
