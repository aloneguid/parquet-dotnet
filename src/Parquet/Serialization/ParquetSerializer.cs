using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Parquet.Schema;

namespace Parquet.Serialization {

    /// <summary>
    /// High-level object serialisation V2. Internal only while being worked on.
    /// Comes as a rewrite of ParquetConvert/ClrBridge/MSILGenerator
    /// </summary>
    internal static class ParquetSerializer {
        public static async Task<ParquetSchema> SerializeAsync<T>(IEnumerable<T> objectInstances, Stream destination,
            ParquetSerializerOptions? options = null) {

            ParquetSchema schema = typeof(T).GetParquetSchema(false);
            DataField[] dataFields = schema.GetDataFields();

            foreach(DataField df in dataFields) {

            }

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, destination)) {

            }

            return schema;
        }

        private static void GenerateColumnCounterDelegate(DataField df) {

        }
    }
}
