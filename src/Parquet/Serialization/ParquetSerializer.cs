using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Extensions;
using Parquet.Meta;
using Parquet.Schema;
using Parquet.Serialization.Dremel;
using Type = System.Type;

namespace Parquet.Serialization {
    /// <summary>
    /// High-level object serialisation.
    /// Comes as a rewrite of ParquetConvert/ClrBridge/MSILGenerator and supports nested types as well.
    /// </summary>
    public static class ParquetSerializer {
        private static readonly Dictionary<(Type, ParquetCompabilityOptions), object> _typeToStriper = new();
        private static readonly ConcurrentDictionary<Type, Dictionary<ParquetSchema, object>> _typeToAssembler = new();

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

            bool append = options != null && options.Append;
            ParquetCompabilityOptions compabilityOptions = ParquetCompabilityOptions.Latest;

            if(append && destination.Length != 0) {
                using ParquetReader reader = await ParquetReader.CreateAsync(destination, leaveStreamOpen: true, cancellationToken: cancellationToken);
                compabilityOptions = reader.Metadata?.GetCompabilityOptions() ?? ParquetCompabilityOptions.Latest;
            }
            
            if(_typeToStriper.TryGetValue((typeof(T), compabilityOptions), out object? boxedStriper)) {
                striper = (Striper<T>)boxedStriper;
            } else {
                striper = new Striper<T>(typeof(T).GetParquetSchema(false, compabilityOptions));
                _typeToStriper[(typeof(T), compabilityOptions)] = striper;
            }

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
            
            var result = new List<T>();
            
            using ParquetReader reader = await ParquetReader.CreateAsync(source, options, cancellationToken: cancellationToken);
            Assembler<T> asm = GetAssembler<T>(reader.Metadata);
            
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
            var result = new List<T>();

            using ParquetReader reader = await ParquetReader.CreateAsync(source, options, cancellationToken: cancellationToken);
            Assembler<T> asm = GetAssembler<T>(reader.Metadata);

            for(int rgi = 0; rgi < reader.RowGroupCount; rgi++) {
                await DeserializeRowGroupAsync(reader, rgi, asm, result, cancellationToken);
            }

            return result;
        }

        private static Assembler<T> GetAssembler<T>(FileMetaData? metadata) where T : new() {
            if(metadata is null) {
                return GetAssembler<T>();
            }
            
            Dictionary<ParquetSchema, object> schemaAssemblersDict = _typeToAssembler.GetOrAdd(
                typeof(T), 
                static _ => new Dictionary<ParquetSchema, object>());
            
            ParquetSchema typeSchema = typeof(T).GetParquetSchema(true, metadata!.GetCompabilityOptions());

            Assembler<T> asm;

            if(schemaAssemblersDict.TryGetValue(typeSchema, out object? boxedAssembler)) {
                asm = (Assembler<T>)boxedAssembler;
            } else {
                asm = new Assembler<T>(typeSchema);
                schemaAssemblersDict[typeSchema] = asm;
            }
            
            return asm;
        }

        private static Assembler<T> GetAssembler<T>() where T : new() {
            Assembler<T> asm;

            Dictionary<ParquetSchema, object> schemaAssemblersDict = _typeToAssembler.GetOrAdd(
                typeof(T), 
                static _ => new Dictionary<ParquetSchema, object>());

            ParquetSchema typeSchema = typeof(T).GetParquetSchema(true);

            if(schemaAssemblersDict.TryGetValue(typeSchema, out object? boxedAssembler)) {
                asm = (Assembler<T>)boxedAssembler;
            } else {
                asm = new Assembler<T>(typeSchema);
                schemaAssemblersDict[typeSchema] = asm;
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
                if(fasm.Field.ClrType == typeof(string) && reader.Schema.DataFields.Any(f => f.Path.Equals(fasm.Field.Path) && f.IsNullable != fasm.Field.IsNullable)) {
                    fasm.Field.IsNullable = !fasm.Field.IsNullable;
                    fasm.Field.PropagateLevels(0, 0);
                }

                DataColumn dc = await rg.ReadColumnAsync(fasm.Field, cancellationToken);
                try {
                    fasm.Assemble(result.Skip(prevRowCount), dc);
                } catch(Exception ex) {
                    throw new InvalidOperationException(
                        $"failed to deserialize column '{fasm.Field.Path}', pseudo code: ['{fasm.IterationExpression.GetPseudoCode()}']",
                        ex);
                }
            }
        }
    }
}