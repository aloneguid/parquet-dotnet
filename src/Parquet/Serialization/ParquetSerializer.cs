using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Schema;
using Parquet.Serialization.Dremel;
using Type = System.Type;

namespace Parquet.Serialization;

/// <summary>
/// High-level object serialisation. Comes as a rewrite of ParquetConvert/ClrBridge/MSILGenerator and supports nested
/// types as well.
/// </summary>
public static class ParquetSerializer {

    // Define a cache of compiled strippers and assemblers.
    // Strong typed serialization uses System.Type map, whereas untyped uses ParquetSchema map.
    private static readonly ConcurrentDictionary<Type, object> _typeToStriper = new();
    private static readonly ConcurrentDictionary<ParquetSchema, object> _schemaToStriper = new();
    private static readonly ConcurrentDictionary<Type, object> _typeToAssembler = new();
    private static readonly ConcurrentDictionary<ParquetSchema, object> _schemaToAssembler = new();
    private static readonly Dictionary<Type, HashSet<Type>> AllowedDeserializerConversions = new() {
        { typeof(DateOnly), new HashSet<Type>{ typeof(DateTime) } },
        { typeof(TimeOnly), new HashSet<Type>{ typeof(TimeSpan) } },
    };

    private static async Task SerializeRowGroupAsync<T>(ParquetWriter writer, Striper<T> striper,
        IEnumerable<T> objectInstances,
        CancellationToken cancellationToken) {

        using ParquetRowGroupWriter rg = writer.CreateRowGroup();

        foreach(FieldStriper<T> fs in striper.FieldStripers) {
            try {
                ShreddedColumn sc = fs.Stripe(fs.Field, objectInstances);
                await sc.CallWriteAsync(fs.Field, rg);
            } catch(Exception ex) {
                throw new ApplicationException($"failed to serialise data column '{fs.Field.Path}'", ex);
            }
        }

        rg.CompleteValidate();
    }

    private static async Task SerializeRowGroupAsync(ParquetWriter writer,
        Striper<IDictionary<string, object?>> striper,
        ParquetSchema schema,
        IReadOnlyCollection<IDictionary<string, object?>> data,
        CancellationToken cancellationToken) {

        using ParquetRowGroupWriter rg = writer.CreateRowGroup();

        foreach(FieldStriper<IDictionary<string, object?>> fs in striper.FieldStripers) {
            try {
                ShreddedColumn sc = fs.Stripe(fs.Field, data);
                await sc.CallWriteAsync(fs.Field, rg);
            } catch(Exception ex) {
                throw new ApplicationException($"failed to serialise data column '{fs.Field.Path}'", ex);
            }
        }

        rg.CompleteValidate();
    }

    #region [ Serialization ]

    /// <summary>
    /// Serialize object instances into Parquet format and write to provided destination stream.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="objectInstances"></param>
    /// <param name="destination"></param>
    /// <param name="options"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="ApplicationException"></exception>
    public static async Task<ParquetSchema> SerializeAsync<T>(IEnumerable<T> objectInstances,
        Stream destination,
        ParquetSerializerOptions? options = null,
        CancellationToken cancellationToken = default) {

        object boxedStriper = _typeToStriper.GetOrAdd(typeof(T), _ => new Striper<T>(typeof(T).GetParquetSchema(false)));
        var striper = (Striper<T>)boxedStriper;

        bool append = options != null && options.Append;
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(striper.Schema, destination,
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
    /// Serialize untyped data (collection of dictionaries) into Parquet format and write to provided destination
    /// stream. Schema must be provided, as it can't be inferred from data.
    /// </summary>
    /// <remarks>
    /// This method is the slowest, use only when necessary.
    /// </remarks>
    public static async Task SerializeUntypedAsync(IReadOnlyCollection<IDictionary<string, object?>> data,
        ParquetSchema schema,
        Stream destination,
        ParquetSerializerOptions? options = null,
        CancellationToken cancellationToken = default) {

        object boxedStriper = _schemaToStriper.GetOrAdd(schema, _ => new Striper<IDictionary<string, object?>>(schema));
        var striper = (Striper<IDictionary<string, object?>>)boxedStriper;

        bool append = options != null && options.Append;
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, destination,
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
                foreach(IDictionary<string, object?>[] chunk in data.Chunk(rgs)) {
                    await SerializeRowGroupAsync(writer, striper, schema, chunk, cancellationToken);
                }
            } else {
                await SerializeRowGroupAsync(writer, striper, schema, data, cancellationToken);
            }
        }
    }

    /// <summary>
    /// Serialize object instances into Parquet format and write to a local file. If file already exists, it will be
    /// overwritten, unless ParquetSerializerOptions.Append is set to true. In this case, data will be appended to
    /// existing file, but only if existing file's schema is compatible with data schema. Otherwise, an exception will
    /// be thrown.
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

    #endregion

    #region [ Deserialization ]

    /// <summary>
    /// Deserialise
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="source"></param>
    /// <param name="options"></param>
    /// <param name="rowGroupIndex">When specified, only reads specified row group.</param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static async Task<IList<T>> DeserializeAsync<T>(Stream source,
        ParquetSerializerOptions? options = null,
        int? rowGroupIndex = null,
        CancellationToken cancellationToken = default)
        where T : class, new() {

        Assembler<T> asm = GetAssembler<T>();

        await using ParquetReader reader = await ParquetReader.CreateAsync(source, options?.ParquetOptions, cancellationToken: cancellationToken);

        long? requestedCapacity = reader.Metadata?.RowGroups.Sum(x => x.NumRows);
        List<T> result = GetList<T>(requestedCapacity);

        for(int rgi = 0; rgi < reader.RowGroupCount; rgi++) {

            if(rowGroupIndex.HasValue && rgi != rowGroupIndex.Value) {
                continue;
            }

            await DeserializeRowGroupAsync(reader, rgi, asm, result, options, cancellationToken);
        }

        return result;
    }

    /// <summary>
    /// Deserialise from local file.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="filePath">File path</param>
    /// <param name="options">Options</param>
    /// <param name="rowGroupIndex">When specified, only reads specified row group.</param>
    /// <param name="cancellationToken">Optional cancellation token</param>
    /// <returns></returns>
    public static async Task<IList<T>> DeserializeAsync<T>(string filePath,
        ParquetSerializerOptions? options = null,
        int? rowGroupIndex = null,
        CancellationToken cancellationToken = default)
        where T : class, new() {
        using FileStream fs = System.IO.File.OpenRead(filePath);
        return await DeserializeAsync<T>(fs, options, rowGroupIndex, cancellationToken);
    }

    #endregion

    /// <summary>
    /// Deserialise untyped data (collection of dictionaries) from Parquet format read from provided source stream.
    /// Schema is returned as well, as it can't be inferred from data.
    /// </summary>
    public static async Task<(IList<Dictionary<string, object>> Data, ParquetSchema Schema)> DeserializeUntypedAsync(Stream source,
        ParquetSerializerOptions? options = null,
        int? rowGroupIndex = null,
        CancellationToken cancellationToken = default) {

        // we can't get assembler in here until we know the schema.

        var result = new List<Dictionary<string, object>>();

        await using ParquetReader reader = await ParquetReader.CreateAsync(source, options?.ParquetOptions, cancellationToken: cancellationToken);
        ParquetSchema schema = reader.Schema;
        Assembler<Dictionary<string, object>> asm = GetAssembler(schema);
        for(int rgi = 0; rgi < reader.RowGroupCount; rgi++) {
            if(rowGroupIndex.HasValue && rgi != rowGroupIndex.Value) {
                continue;
            }
            await DeserializeRowGroupAsync(reader, rgi, asm, result, options, cancellationToken);
        }

        return (result, schema);
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
        ICollection<T> result, ParquetSerializerOptions? options, CancellationToken cancellationToken, bool resultsAlreadyAllocated = false)
        where T : class, new() {

        using ParquetRowGroupReader rg = reader.OpenRowGroupReader(rgi);

        await DeserializeRowGroupAsync(rg, reader.Schema, asm, result, options, cancellationToken, resultsAlreadyAllocated);
    }

    private static async Task DeserializeRowGroupAsync(ParquetReader reader, int rgi,
        Assembler<Dictionary<string, object>> asm,
        List<Dictionary<string, object>> result,
        ParquetSerializerOptions? options,
        CancellationToken cancellationToken) {
        using ParquetRowGroupReader rg = reader.OpenRowGroupReader(rgi);
        await DeserializeRowGroupAsync(rg, reader.Schema, asm, result, options, cancellationToken);
    }

    private static DataField? MakeForReading(ParquetSchema fileSchema, DataField assemblerField, ParquetSerializerOptions? options) {
        DataField? fileField;

        if(options != null && options.PropertyNameCaseInsensitive) {
            // case insensitive search
            string path = assemblerField.Path.ToString();
            fileField = fileSchema.DataFields.FirstOrDefault(f => f.Path.ToString().Equals(path, StringComparison.OrdinalIgnoreCase));
        } else {
            fileField = fileSchema.DataFields.FirstOrDefault(f => f.Path.Equals(assemblerField.Path));
        }

        if(fileField == null)
            return null;

        // validate "absolute must" in schema compatibility
        if(fileField.MaxDefinitionLevel != assemblerField.MaxDefinitionLevel)
            throw new InvalidDataException($"class definition level ({assemblerField.MaxDefinitionLevel}) does not match file's definition level ({fileField.MaxDefinitionLevel}) in field '{assemblerField.Path}'. This usually means nullability in class definiton is incompatible.");

        if(fileField.MaxRepetitionLevel != assemblerField.MaxRepetitionLevel)
            throw new InvalidDataException($"class repetition level ({assemblerField.MaxRepetitionLevel}) does not match file's repetition level ({fileField.MaxRepetitionLevel}) in field '{assemblerField.Path}'. This usually means collection in class definition is incompatible.");

        if(fileField.ClrType != assemblerField.ClrType) {

            // check if this is one of the allowed conversions
            bool isStillAllowed =
                AllowedDeserializerConversions.TryGetValue(assemblerField.ClrType, out HashSet<Type>? allowedConversions) &&
                allowedConversions.Contains(fileField.ClrType);

            if(!isStillAllowed)
                throw new InvalidCastException($"class type ({assemblerField.ClrType}) does not match file's type ({fileField.ClrType}) in field '{assemblerField.Path}'");
        }


        // make final result
        DataField r = (DataField)assemblerField.Clone();
        r.Path = fileField.Path;

        return r;
    }

    private static async Task DeserializeRowGroupAsync<T>(ParquetRowGroupReader rg,
        ParquetSchema schema,
        Assembler<T> asm,
        ICollection<T> result,
        ParquetSerializerOptions? options,
        CancellationToken cancellationToken = default,
        bool resultsAlreadyAllocated = false) where T : class, new() {

        // add more empty class instances to the result
        int prevRowCount = result.Count;

        if(!resultsAlreadyAllocated) {
            for(int i = 0; i < rg.RowCount; i++) {
                var ne = new T();
                result.Add(ne);
            }
        }

        foreach(FieldAssembler<T> fasm in asm.FieldAssemblers) {

            DataField? readerField = MakeForReading(schema, fasm.Field, options);

            // skips column deserialisation if it doesn't exist in file's schema
            if(readerField == null)
                continue;


            // this needs reflected schema field due to it containing important schema adjustments
            object rawColumnDataOfT;

            try {
                rawColumnDataOfT = await rg.ReadRawColumnDataAsObjectAsync(readerField, cancellationToken);
            } catch(Exception ex) {
                throw new InvalidDataException($"failed to read column '{fasm.Field.Path}'", ex);
            }

            try {
                fasm.Assemble(resultsAlreadyAllocated ? result : result.Skip(prevRowCount), rawColumnDataOfT);
            } catch(Exception ex) {
                throw new InvalidOperationException($"failed to deserialize column '{fasm.Field.Path}'", ex);
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