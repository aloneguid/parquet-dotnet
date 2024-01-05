using System.Collections.Generic;
using System.Linq;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet.Floor.ViewModels;

public class FieldModel {
    public FieldModel(Field field) {
        Field = field;
    }

    public Field Field { get; }

    public string Name => Field.Name;

    public string? NumChildren => Field.SchemaElement?.NumChildren?.ToString();

    public string? Type => FormatDataType();

    public string? TypeLength => Field.SchemaElement?.TypeLength.ToString();

    public string? RepetitionType => Field.SchemaElement?.RepetitionType.ToString();

    public string? ConvertedType => Field.SchemaElement?.ConvertedType?.ToString();

    public string? Scale => Field.SchemaElement?.Scale?.ToString();

    public string? Precision => Field.SchemaElement?.Precision?.ToString();

    public string? FieldId => Field.SchemaElement?.FieldId?.ToString();

    public string? LogicalType => FormatLogicalType();

    public string DefinitionLevel => Field.MaxDefinitionLevel.ToString();

    public string RepetitionLevel => Field.MaxRepetitionLevel.ToString();

    public List<FieldModel> Children => Field switch {
        StructField sf => sf.Fields.Select(f => new FieldModel(f)).ToList(),
        MapField mf => new List<FieldModel> {
            new FieldModel(mf.Key),
            new FieldModel(mf.Value)
        },
        ListField lf => new List<FieldModel> {
            new FieldModel(lf.Item)
        },
        _ => new List<FieldModel>()
    };


    private string? FormatDataType() {
        if(Field is DataField df) {
            return df.SchemaElement?.Type.ToString();
        } else if(Field is StructField sf) {
            return "struct";
        } else if(Field is MapField mf) {
            return "map";
        } else if(Field is ListField lf) {
            return "list";
        } else {
            return "unknown";
        }
    }

    private string FormatLogicalType() {

        LogicalType? lt = Field.SchemaElement?.LogicalType;

        if(lt == null)
            return string.Empty;

        if(lt.UUID != null)
            return "UUID";

        if(lt.STRING != null)
            return "STRING";

        if(lt.MAP != null)
            return "MAP";

        if(lt.LIST != null)
            return "LIST";

        if(lt.ENUM != null)
            return "ENUM";

        if(lt.DECIMAL != null)
            return $"DECIMAL (precision: {lt.DECIMAL.Precision}, scale: {lt.DECIMAL.Scale})";

        if(lt.DATE != null)
            return $"DATE";

        if(lt.TIME != null) {
            string unit = lt.TIME.Unit.MICROS != null
                ? "MICROS"
                : lt.TIME.Unit.MILLIS != null
                    ? "MILLIS"
                    : "NANOS";
            return $"TIME (unit: {unit}, isAdjustedToUTC: {lt.TIME.IsAdjustedToUTC})";
        }

        if(lt.TIMESTAMP != null)
            return "TIMESTAMP";

        if(lt.INTEGER != null)
            return $"INTEGER (bitWidth: {lt.INTEGER.BitWidth}, isSigned: {lt.INTEGER.IsSigned})";

        if(lt.UNKNOWN != null)
            return "UNKNOWN";

        if(lt.JSON != null)
            return "JSON";

        if(lt.BSON != null)
            return "BSON";

        if(lt.UUID != null)
            return "UUID";

        return "?";
    }

}
