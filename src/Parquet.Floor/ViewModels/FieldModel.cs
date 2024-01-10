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

    public string? LogicalType => Field.SchemaElement?.LogicalType?.ToSimpleString();

    public string DefinitionLevel => Field.MaxDefinitionLevel.ToString();

    public string RepetitionLevel => Field.MaxRepetitionLevel.ToString();

    public bool IsExpanded { get; set; } = true;

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
}
