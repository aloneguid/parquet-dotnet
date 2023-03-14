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
    /// High-level object serialisation V2. Internal only while being worked on.
    /// Comes as a rewrite of ParquetConvert/ClrBridge/MSILGenerator and supports nested types as well.
    /// TODO:
    /// - lists
    /// - maps
    /// - structs
    /// - append to file
    /// </summary>
    internal static class ParquetSerializer {
        public static async Task<ParquetSchema> SerializeAsync<T>(IEnumerable<T> objectInstances, Stream destination,
            ParquetSerializerOptions? options = null,
            CancellationToken cancellationToken = default) {

            Striper<T> striper = new Striper<T>(typeof(T).GetParquetSchema(false));

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(striper.Schema, destination)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();

                foreach(FieldStriper<T> fs in striper.FieldStripers) {
                    DataColumn dc;
                    try {
                        ShreddedColumn sc = fs.Stripe(fs.Field, objectInstances);
                        dc = new DataColumn(fs.Field, sc.Data, sc.DefinitionLevels, sc.RepetitionLevels);
                        await rg.WriteColumnAsync(dc, cancellationToken);
                    } catch(Exception ex) {
                        throw new ApplicationException($"failed to serialise data column '{fs.Field.Path}'", ex);
                    }
                }
            }

            return striper.Schema;
        }

        public static async Task<ParquetSchema> SerializeAsync<T>(IEnumerable<T> objectInstances, string filePath,
            ParquetSerializerOptions? options = null,
            CancellationToken cancellationToken = default) {
            using FileStream fs = System.IO.File.Create(filePath);
            return await SerializeAsync(objectInstances, fs, options, cancellationToken);
        }

        public static async Task<IList<T>> DeserializeAsync<T>(Stream source,
            CancellationToken cancellationToken = default)
            where T : new() {

            Assembler<T> asm = new Assembler<T>(typeof(T).GetParquetSchema(true));
            var result = new List<T>();

            using ParquetReader reader = await ParquetReader.CreateAsync(source, new ParquetOptions { UnpackDefinitions = false });
            for(int rgi = 0; rgi < reader.RowGroupCount; rgi++) {
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
                        throw new InvalidOperationException($"failed to deserialise column '{fasm.Field.Path}', pseude code: ['{fasm.IterationExpression.GetPseudoCode()}']", ex);
                    }
                }
            }

            return result;
        }
    }
}
