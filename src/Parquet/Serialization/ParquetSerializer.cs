using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Extensions;
using Parquet.Schema;
using Parquet.Serialization.Dremel;

namespace Parquet.Serialization {

    /// <summary>
    /// High-level object serialisation.
    /// Comes as a rewrite of ParquetConvert/ClrBridge/MSILGenerator and supports nested types as well.
    /// </summary>
    public static class ParquetSerializer {

        private static readonly Dictionary<Type, object> _typeToStriper = new();
        private static readonly Dictionary<Type, object> _typeToAssembler = new();

        private static async Task SerializeRowGroupAsync<T>(ParquetWriter writer, Striper<T> striper,
            IEnumerable<T> objectInstances,
            CancellationToken cancellationToken) {

            using ParquetRowGroupWriter rg = writer.CreateRowGroup();

            foreach(FieldStriper<T> fs in striper.FieldStripers) {
                DataColumn dc;
                try {
                    ShreddedColumn sc = fs.Stripe(fs.Field, objectInstances);
                    dc = new DataColumn(fs.Field, sc.Data, sc.DefinitionLevels?.ToArray(), sc.RepetitionLevels?.ToArray());
                    await rg.WriteColumnAsync(dc, cancellationToken);
                } catch(Exception ex) {
                    throw new ApplicationException($"failed to serialise data column '{fs.Field.Path}'", ex);
                }
            }
        }

        /// <summary>
        /// Serialize 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="objectInstances"></param>
        /// <param name="destination"></param>
        /// <param name="options"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="ApplicationException"></exception>
        public static async Task<ParquetSchema> SerializeAsync<T>(IEnumerable<T> objectInstances, Stream destination,
            ParquetSerializerOptions? options = null,
            CancellationToken cancellationToken = default) {

            Striper<T> striper;

            if(_typeToStriper.TryGetValue(typeof(T), out object? boxedStriper)) {
                striper = (Striper<T>)boxedStriper;
            } else {
                striper = new Striper<T>(typeof(T).GetParquetSchema(false));
                _typeToStriper[typeof(T)] = striper;
            }

            bool append = options != null && options.Append;
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(striper.Schema, destination,
                options?.ParquetOptions,
                append, cancellationToken)) {

                if(options != null) {
                    writer.CompressionMethod = options.CompressionMethod;
                    writer.CompressionLevel = options.CompressionLevel;
                }

                if(options?.RowGroupSize != null) {
                    int rgs = options.RowGroupSize.Value;
                    if(rgs < 1)
                        throw new InvalidOperationException($"row group size must be a positive number, but passed {rgs}");
                    foreach(T[] chunk in objectInstances.Chunk(rgs)) {
                        await SerializeRowGroupAsync<T>(writer, striper, chunk, cancellationToken);
                    }
                } else {
                    await SerializeRowGroupAsync<T>(writer, striper, objectInstances, cancellationToken);
                }
            }

            return striper.Schema;
        }

        /// <summary>
        /// Serialise
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="objectInstances"></param>
        /// <param name="filePath"></param>
        /// <param name="options"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<ParquetSchema> SerializeAsync<T>(IEnumerable<T> objectInstances, string filePath,
            ParquetSerializerOptions? options = null,
            CancellationToken cancellationToken = default) {
            using FileStream fs = System.IO.File.Create(filePath);
            return await SerializeAsync(objectInstances, fs, options, cancellationToken);
        }


        /// <summary>
        /// Deserialise row group
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="rowGroupIndex"></param>
        /// <param name="options"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public static async Task<IList<T>> DeserializeAsync<T>(Stream source,
            int rowGroupIndex,
            ParquetOptions? options = null,
            CancellationToken cancellationToken = default)
            where T : new() {

            Assembler<T> asm = GetAssembler<T>();

            var result = new List<T>();
            using ParquetReader reader = await ParquetReader.CreateAsync(source, options, cancellationToken: cancellationToken);
            await DeserializeRowGroupAsync(reader, rowGroupIndex, asm, result, cancellationToken);

            return result;
        }

        /// <summary>
        /// Deserialise
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="options"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public static async Task<IList<T>> DeserializeAsync<T>(Stream source,
            ParquetOptions? options = null,
            CancellationToken cancellationToken = default)
            where T : new() {

            Assembler<T> asm = GetAssembler<T>();

            var result = new List<T>();

            using ParquetReader reader = await ParquetReader.CreateAsync(source, options, cancellationToken: cancellationToken);
            for(int rgi = 0; rgi < reader.RowGroupCount; rgi++) {
                await DeserializeRowGroupAsync(reader, rgi, asm, result, cancellationToken);
            }

            return result;
        }

        private static Assembler<T> GetAssembler<T>() where T : new() {

            Assembler<T> asm;

            if(_typeToAssembler.TryGetValue(typeof(T), out object? boxedAssembler)) {
                asm = (Assembler<T>)boxedAssembler;
            } else {
                asm = new Assembler<T>(typeof(T).GetParquetSchema(true));
                _typeToAssembler[typeof(T)] = asm;
            }

            return asm;
        }

        private static async Task DeserializeRowGroupAsync<T>(ParquetReader reader, int rgi,
            Assembler<T> asm,
            ICollection<T> result,
            CancellationToken cancellationToken = default) where T : new() {

            using ParquetRowGroupReader rg = reader.OpenRowGroupReader(rgi);

            // add more empty class instances to the result
            int prevRowCount = result.Count;
            for(int i = 0; i < rg.RowCount; i++) {
                var ne = new T();
                result.Add(ne);
            }

            foreach(FieldAssembler<T> fasm in asm.FieldAssemblers) {
                DataColumn dc = await rg.ReadColumnAsync(fasm.Field, cancellationToken);
                try {
                    fasm.Assemble(result.Skip(prevRowCount), dc);
                } catch(Exception ex) {
                    throw new InvalidOperationException($"failed to deserialize column '{fasm.Field.Path}', pseudo code: ['{fasm.IterationExpression.GetPseudoCode()}']", ex);
                }
            }
        }
    }
}