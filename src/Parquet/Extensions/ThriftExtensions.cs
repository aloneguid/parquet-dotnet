using System.Linq;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet; 

/// <summary>
/// Internal data structure helpers
/// </summary>
static class ThriftExtensions {
    public static bool IsAnnotatedWithAny(this SchemaElement schemaElement, ConvertedType[] convertedTypes) {
        if(convertedTypes == null || convertedTypes.Length == 0)
            return false;

        return
           schemaElement.ConvertedType != null &&
           convertedTypes.Any(ct => ct == schemaElement.ConvertedType);
    }

    public static bool IsList(this SchemaElement? se) {
        if(se == null) return false;

        return se.LogicalType?.LIST != null || se.ConvertedType == ConvertedType.LIST;
    }

    public static bool IsMap(this SchemaElement? se) {
        if(se == null) return false;
        return se.LogicalType?.MAP != null || se.ConvertedType == ConvertedType.MAP || se.ConvertedType == ConvertedType.MAP_KEY_VALUE;
    }

    public static bool IsNullable(this SchemaElement schemaElement) {
        return schemaElement.RepetitionType != FieldRepetitionType.REQUIRED;
    }

    public static FieldPath GetPath(this ColumnChunk columnChunk) {
        return new FieldPath(columnChunk.MetaData!.PathInSchema);
    }

    public static string Describe(this SchemaElement se) {
        return $"[n: {se.Name}, t: {se.Type}, ct: {se.ConvertedType}, rt: {se.RepetitionType}, c: {se.NumChildren}]";
    }
}