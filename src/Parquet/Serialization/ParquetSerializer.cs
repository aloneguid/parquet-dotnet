using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
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

        // Define a cache of compiled strippers and assemblers.
        // Strong typed serialization uses System.Type map, whereas untyped uses ParquetSchema map.
        private static readonly ConcurrentDictionary<Type, object> _typeToStriper = new();
        private static readonly ConcurrentDictionary<ParquetSchema,  object> _schemaToStriper = new();
        private static readonly ConcurrentDictionary<Type, object> _typeToAssembler = new();
        private static readonly ConcurrentDictionary<ParquetSchema, object> _schemaToAssembler = new();

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

        private static async Task SerializeRowGroupAsync(ParquetWriter writer,
            Striper<IDictionary<string, object>> striper,
            ParquetSchema schema,
            IReadOnlyCollection<IDictionary<string, object>> data,
            CancellationToken cancellationToken) {

            using ParquetRowGroupWriter rg = writer.CreateRowGroup();

            foreach(FieldStriper<IDictionary<string, object>> fs in striper.FieldStripers) {
                DataColumn dc;
                try {
                    ShreddedColumn sc = fs.Stripe(fs.Field, data);
                    dc = new DataColumn(fs.Field, sc.Data, sc.DefinitionLevels?.ToArray(), sc.RepetitionLevels?.ToArray());
                    await rg.WriteColumnAsync(dc, cancellationToken);
                } catch(Exception ex) {
                    throw new ApplicationException($"failed to serialise data column '{fs.Field.Path}'", ex);
                }
            }
        }

        /// <summary>
        /// Serialize a collection into one row group using an existing writer.
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="objectInstances"></param>
        /// <param name="cancellationToken"></param>
        /// <typeparam name="T"></typeparam>
        public static async Task SerializeRowGroupAsync<T>(ParquetWriter writer, IEnumerable<T> objectInstances,
            CancellationToken cancellationToken) {
            using ParquetRowGroupWriter rowGroupWritter = writer.CreateRowGroup();
            object boxedStriper = _typeToStriper.GetOrAdd(typeof(T), _ => new Striper<T>(typeof(T).GetParquetSchema(false)));
            var striper = (Striper<T>)boxedStriper;

            await SerializeRowGroupAsync(writer, striper, objectInstances, cancellationToken);
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

            object boxedStriper = _typeToStriper.GetOrAdd(typeof(T), _ => new Striper<T>(typeof(T).GetParquetSchema(false)));
            var striper = (Striper<T>)boxedStriper;

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
        /// Experimental object serialisation
        /// </summary>
        public static async Task SerializeAsync(ParquetSchema schema,
            IReadOnlyCollection<IDictionary<string, object>> data,
            Stream destination,
            ParquetSerializerOptions? options = null,
            CancellationToken cancellationToken = default) {


            object boxedStriper = _schemaToStriper.GetOrAdd(schema, _ => new Striper<IDictionary<string, object>>(schema));
            var striper = (Striper<IDictionary<string, object>>)boxedStriper;

            bool append = options != null && options.Append;
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, destination,
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
                    foreach(IDictionary<string, object>[] chunk in data.Chunk(rgs)) {
                        await SerializeRowGroupAsync(writer, striper, schema, chunk, cancellationToken);
                    }
                } else {
                    await SerializeRowGroupAsync(writer, striper, schema, data, cancellationToken);
                }
            }
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
            using FileStream fs = (options?.Append ?? false) 
                ? System.IO.File.Open(filePath, FileMode.Open, FileAccess.ReadWrite) 
                : System.IO.File.Create(filePath);
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

            using ParquetReader reader = await ParquetReader.CreateAsync(source, options, cancellationToken: cancellationToken);

            List<T> result = GetList<T>(reader.Metadata?.RowGroups[rowGroupIndex].NumRows);
            await DeserializeRowGroupAsync(reader, rowGroupIndex, asm, result, cancellationToken);

            return result;
        }

        /// <summary>
        /// Deserialize row group into provided list
        /// </summary>
        /// <param name="source"></param>
        /// <param name="rowGroupIndex"></param>
        /// <param name="result"></param>
        /// <param name="options"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="resultsAlreadyAllocated">Set to true if provided result collection already contains allocated elements (like using a pool)</param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public static async Task DeserializeAsync<T>(Stream source, int rowGroupIndex, IList<T> result, 
            ParquetOptions? options = null, CancellationToken cancellationToken = default,
            bool resultsAlreadyAllocated = false) where T : new() {

            using ParquetReader reader =
                await ParquetReader.CreateAsync(source, options, cancellationToken: cancellationToken);

            await DeserializeAsync(reader, rowGroupIndex, result, cancellationToken, resultsAlreadyAllocated);
        }

        /// <summary>
        /// Deserialize row group into provided list, using provided Parquet reader
        /// Useful to iterate over row group using a single parquet reader
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="rowGroupIndex"></param>
        /// <param name="result"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="resultsAlreadyAllocated">Set to true if provided result collection already contains allocated elements (like using a pool)</param>
        /// <typeparam name="T"></typeparam>
        public static async Task DeserializeAsync<T>(ParquetReader reader, int rowGroupIndex, IList<T> result,
            CancellationToken cancellationToken = default, bool resultsAlreadyAllocated = false) where T : new() {
            Assembler<T> asm = GetAssembler<T>();
            await DeserializeRowGroupAsync(reader, rowGroupIndex, asm, result, cancellationToken, 
                resultsAlreadyAllocated);
        }

        /// <summary>
        /// Deserialize a specific row group from a local file.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filePath">Local file path</param>
        /// <param name="rowGroupIndex">Zero-based row group index</param>
        /// <param name="options">Parquet options</param>
        /// <param name="cancellationToken">Optional cancellation token</param>
        /// <returns></returns>
        public static async Task<IList<T>> DeserializeAsync<T>(string filePath,
            int rowGroupIndex,
            ParquetOptions? options = null,
            CancellationToken cancellationToken = default)
            where T : new() {
            using FileStream fs = System.IO.File.OpenRead(filePath);
            return await DeserializeAsync<T>(fs, rowGroupIndex, options, cancellationToken);
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

            using ParquetReader reader = await ParquetReader.CreateAsync(source, options, cancellationToken: cancellationToken);

            long? requestedCapacity = reader.Metadata?.RowGroups.Sum(x => x.NumRows);
            List<T> result = GetList<T>(requestedCapacity);

            for(int rgi = 0; rgi < reader.RowGroupCount; rgi++) {

                await DeserializeRowGroupAsync(reader, rgi, asm, result, cancellationToken);
            }

            return result;
        }

        /// <summary>
        /// Deserialise from local file.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filePath">File path</param>
        /// <param name="options">Options</param>
        /// <param name="cancellationToken">Optional cancellation token</param>
        /// <returns></returns>
        public static async Task<IList<T>> DeserializeAsync<T>(string filePath,
            ParquetOptions? options = null,
            CancellationToken cancellationToken = default)
            where T : new() {
            using FileStream fs = System.IO.File.OpenRead(filePath);
            return await DeserializeAsync<T>(fs, options, cancellationToken);
        }

        /// <summary>
        /// Highly experimental
        /// </summary>
        public record UntypedResult(IList<Dictionary<string, object>> Data, ParquetSchema Schema);

        /// <summary>
        /// Highly experimental
        /// </summary>
        public static async Task<UntypedResult> DeserializeAsync(Stream source,
            ParquetOptions? options = null,
            CancellationToken cancellationToken = default) {

            // we can't get assembler in here until we know the schema.

            var result = new List<Dictionary<string, object>>();

            using ParquetReader reader = await ParquetReader.CreateAsync(source, options, cancellationToken: cancellationToken);
            ParquetSchema schema = reader.Schema;
            Assembler<Dictionary<string, object>> asm = GetAssembler(schema);
            for(int rgi = 0; rgi < reader.RowGroupCount; rgi++) {
                await DeserializeRowGroupAsync(reader, rgi, asm, result, cancellationToken);
            }

            return new UntypedResult(result, schema);
        }

        /// <summary>
        /// Deserialize as async enumerable
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="options"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async IAsyncEnumerable<T> DeserializeAllAsync<T>(Stream source,
            ParquetOptions? options = null,
            [EnumeratorCancellation]CancellationToken cancellationToken = default)
            where T : new() {

            Assembler<T> asm = GetAssembler<T>();

            using ParquetReader reader = await ParquetReader.CreateAsync(source, options, cancellationToken: cancellationToken);

            long? requestedCapacity = reader.Metadata?.RowGroups.Max(x => x.NumRows);
            List<T> result = GetList<T>(requestedCapacity);

            for(int rgi = 0; rgi < reader.RowGroupCount; rgi++) {

                await DeserializeRowGroupAsync(reader, rgi, asm, result, cancellationToken);
                foreach (T? item in result) {
                    yield return item;
                }

                result.Clear();
            }
        }

        /// <summary>
        /// Deserialise
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rowGroupReader"></param>
        /// <param name="schema"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        /// <exception cref="InvalidDataException"></exception>
        public static async Task<IList<T>> DeserializeAsync<T>(ParquetRowGroupReader rowGroupReader,
            ParquetSchema schema,
            CancellationToken cancellationToken = default)
            where T : new() {
            List<T> result = GetList<T>(rowGroupReader.RowGroup.NumRows);

            await DeserializeRowGroupAsync(rowGroupReader, schema, result, cancellationToken);

            return result;
        }

        /// <summary>
        /// Deserialize a single row group into a provided collection.
        /// </summary>
        /// <param name="rowGroupReader"></param>
        /// <param name="schema"></param>
        /// <param name="result"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="resultsAlreadyAllocated">Set to true if provided result collection already contains allocated elements (like using a pool)</param>
        /// <typeparam name="T"></typeparam>
        public static async Task DeserializeRowGroupAsync<T>(ParquetRowGroupReader rowGroupReader,
            ParquetSchema schema,
            ICollection<T> result,
            CancellationToken cancellationToken = default,
            bool resultsAlreadyAllocated = false) where T : new() {
            Assembler<T> asm = GetAssembler<T>();
            
            await DeserializeRowGroupAsync(rowGroupReader, schema, asm, result, cancellationToken,
                resultsAlreadyAllocated);
        }

        private static Assembler<T> GetAssembler<T>() where T : new() {
            object boxedAssembler = _typeToAssembler.GetOrAdd(typeof(T), _ => new Assembler<T>(typeof(T).GetParquetSchema(true)));
            return (Assembler<T>)boxedAssembler;
        }

        private static Assembler<Dictionary<string, object>> GetAssembler(ParquetSchema schema) {
            object boxedAssembler = _schemaToAssembler.GetOrAdd(schema, _ => new Assembler<Dictionary<string, object>>(schema));
            return (Assembler<Dictionary<string, object>>)boxedAssembler;
        }

        private static async Task DeserializeRowGroupAsync<T>(ParquetReader reader, int rgi, Assembler<T> asm,
            ICollection<T> result, CancellationToken cancellationToken, bool resultsAlreadyAllocated = false)
            where T : new() {

            using ParquetRowGroupReader rg = reader.OpenRowGroupReader(rgi);

            await DeserializeRowGroupAsync(rg, reader.Schema, asm, result, cancellationToken, resultsAlreadyAllocated);
        }

        private static async Task DeserializeRowGroupAsync(ParquetReader reader, int rgi,
            Assembler<Dictionary<string, object>> asm,
            List<Dictionary<string, object>> result,
            CancellationToken cancellationToken) {
            using ParquetRowGroupReader rg = reader.OpenRowGroupReader(rgi);
            await DeserializeRowGroupAsync(rg, reader.Schema, asm, result, cancellationToken);
        }

        private static async Task DeserializeRowGroupAsync<T>(ParquetRowGroupReader rg,
            ParquetSchema schema,
            Assembler<T> asm,
            ICollection<T> result,
            CancellationToken cancellationToken = default,
            bool resultsAlreadyAllocated = false) where T : new() {

            // add more empty class instances to the result
            int prevRowCount = result.Count;
            
            if(!resultsAlreadyAllocated)
                for(int i = 0; i < rg.RowCount; i++) {
                    var ne = new T();
                    result.Add(ne);
                }

            foreach(FieldAssembler<T> fasm in asm.FieldAssemblers) {

                // validate reflected vs actual schema field
                DataField? actual = schema.DataFields.FirstOrDefault(f => f.Path.Equals(fasm.Field.Path));
                if(actual != null && !actual.IsArray && !fasm.Field.Equals(actual)) {
                    throw new InvalidDataException($"property '{fasm.Field.ClrPropName}' is declared as '{fasm.Field}' but source data has it as '{actual}'");
                }

                // skips column deserialisation if it doesn't exist in file's schema
                if(!rg.ColumnExists(fasm.Field)) {
                    continue;
                }

                DataColumn dc = await rg.ReadColumnAsync(fasm.Field, cancellationToken);

                try {
                    fasm.Assemble(resultsAlreadyAllocated ? result : result.Skip(prevRowCount), dc);
                } catch(Exception ex) {
                    throw new InvalidOperationException($"failed to deserialize column '{fasm.Field.Path}', pseudo code: ['{fasm.IterationExpression.GetPseudoCode()}']", ex);
                }
            }
        }

        private static List<T> GetList<T>(long? requestedCapacity) {
            if(requestedCapacity == null)
                return new List<T>();
            
            if(requestedCapacity >= int.MaxValue)
                return new List<T>(int.MaxValue);
            
            return new List<T>((int)requestedCapacity);
        }
    }
}