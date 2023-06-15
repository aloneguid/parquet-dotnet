using System.Linq;
using Parquet.Meta;

namespace Parquet.Serialization {
    internal static class FileMetaDataExtensions {
        internal static ParquetCompabilityOptions GetCompabilityOptions(this FileMetaData meta) {
            bool schemaContainsPrimitivesArrays = meta.Schema
                .Any(el => el.RepetitionType is FieldRepetitionType.REPEATED
                           && (
                               IsPrimitive(el.Type)
                               || el is {
                                   Type: Type.BYTE_ARRAY,
                                   ConvertedType: ConvertedType.UTF8 or ConvertedType.DECIMAL
                               }
                               || el is {
                                   Type: Type.FIXED_LEN_BYTE_ARRAY,
                                   ConvertedType: ConvertedType.DECIMAL or ConvertedType.INTERVAL
                               })
                           );

            ParquetCompabilityOptions opt = ParquetCompabilityOptions.Latest;

            if(schemaContainsPrimitivesArrays) {
                opt |= ParquetCompabilityOptions.MakeArrays;
            }

            return opt;
        }

        private static bool IsPrimitive(Type? type)
            => type is Type.BOOLEAN
                or Type.INT32
                or Type.INT32
                or Type.INT64
                or Type.INT96
                or Type.FLOAT
                or Type.DOUBLE;
    }
}