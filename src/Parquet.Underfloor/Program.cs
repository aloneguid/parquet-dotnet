using Grey;
using Parquet;
using Parquet.Meta;
using Parquet.Schema;
using Parquet.Serialization;
using Parquet.Underfloor;
using static Grey.App;

// global state
WorkFile fd = await WorkFile.CreateAsync(null);

async Task LoadAsync(string path) {

    await fd.DisposeAsync();
    fd = await WorkFile.CreateAsync(path);

    try {
        //using(Stream fileStream = File.OpenRead(path)) {
        //    await ParquetSerializer.DeserializeWithProgressAsync(fileStream, fd,
        //        new ParquetSerializerOptions {
        //            ParquetOptions = new ParquetOptions {
        //                TreatByteArrayAsString = true
        //            }
        //        });
        //}

        //fd.RowGroupCountDisplay = reader.RowGroupCount.ToString("N0");        
        //fd.CustomMetadata = reader.CustomMetadata;
        //fd.RowGroups = reader.RowGroups;
        //fd.RowGroupCountDisplay = reader.RowGroupCount.ToString("N0");

    } catch(Exception ex) {
        fd.ErrorMessage = ex.Message;
    }
}

void LoadFromFile(string path) {
    LoadAsync(path).Forget();
}

if(args.Length == 0) {
    // no file specified, show error message
    fd.ErrorMessage = "Parquet file path needs to be passed as first argument.";
} else if(args.Length == 1) {
    string fileName = args[0];

    if(File.Exists(fileName)) {
        // file exists, load it
        LoadFromFile(fileName);
    } else {
        // file does not exist, show error message
        fd.ErrorMessage = $"File does not exist.";
    }
}

string title = "Parquet Underfloor";
bool showRawSchema = false;

static void RenderNull(int rowIdx, int colIdx) {
    Button($"null##{rowIdx}-{colIdx}", isEnabled: false, isSmall: true);
}

void RenderPrimitiveValue(int rowIdx, int colIdx, DataField df, object value) {
    if(df.ClrType == typeof(bool)) {
        bool b = (bool)value;
        Checkbox($"##{rowIdx}-{colIdx}", ref b);
        return;
    }

    Label(value?.ToString() ?? "");
}

void RenderStruct(int rowIdx, int colIdx, StructField sf, object value) {
    if(value is Dictionary<string, object> dd) {
        foreach(Field f in sf.Fields) {
            Label(f.Name, isEnabled: false); SL();
            dd.TryGetValue(f.Name, out object? v);
            RenderValue(rowIdx, colIdx, f, v);
        }
    }
}

void RenderValue(int rowIdx, int colIdx, Field f, object? value) {
    if(value == null) {
        RenderNull(rowIdx, colIdx);
        return;
    }

    if(f.SchemaType == SchemaType.Data) {
        if(f is DataField df) {
            if(df.IsArray) {
                Label("array");
            } else {
                RenderPrimitiveValue(rowIdx, colIdx, df, value);
            }
        }
    } else if(f.SchemaType == SchemaType.Struct) {
        RenderStruct(rowIdx, colIdx, (StructField)f, value);
    } else {
        Label(f.SchemaType.ToString());
    }
}

void RenderData() {
    /*
    if(fd.Columns == null || fd.ColumnsDisplay == null || fd.Schema == null)
        return;

    int rowCount = (int)(fd.Metadata?.NumRows ?? 0);

    Table("data", fd.ColumnsDisplay, rowCount, (int rowIdx, int colIdx) => {
        //if(fd.DataColumns == null)
        //    return;

        if(colIdx == 0) {
            Label(rowIdx.ToString(), isEnabled: false);
            return; // first column is row index
        }

        colIdx -= 1;
        //Dictionary<string, object> row = fd.Data[rowIdx];
        //string columnName = fd.Columns[colIdx];
        //Field f = fd.Schema[colIdx];
        //row.TryGetValue(columnName, out object? value);
        //RenderValue(rowIdx, colIdx, f, value);
    }, 0, -20, true);
    */
}

void LN(string? icon, string key, string? value) {
    if(value == null)
        return;

    Label(icon ?? "");
    SL(60);
    Label(key, Emphasis.Primary);
    SL(300);
    Label(value);
}

void ALN(string key, string? value) {
    if(value == null)
        return;

    Label(key, Emphasis.Primary);
    SL(350);
    
    // If the value is a number, format it with thousands separator
    if(long.TryParse(value, out long numValue)) {
        Label(numValue.ToString("N0"));
    } else {
        Label(value);
    }
}

// Formats Parquet LogicalType to a short, human-readable uppercase label
string FormatLogicalType(LogicalType? lt) {
    if(lt == null)
        return string.Empty;

    if(lt.STRING != null)
        return "STRING";
    if(lt.UUID != null)
        return "UUID";
    if(lt.MAP != null)
        return "MAP";
    if(lt.LIST != null)
        return "LIST";
    if(lt.ENUM != null)
        return "ENUM";
    if(lt.DECIMAL != null) {
        DecimalType d = lt.DECIMAL;
        return $"DECIMAL({d.Precision},{d.Scale})";
    }
    if(lt.DATE != null)
        return "DATE";

    if(lt.TIME != null) {
        TimeType t = lt.TIME;
        string unit = t.Unit.MILLIS != null ? "MILLIS" : t.Unit.MICROS != null ? "MICROS" : t.Unit.NANOS != null ? "NANOS" : string.Empty;
        string utc = t.IsAdjustedToUTC ? ",UTC" : string.Empty;
        return string.IsNullOrEmpty(unit) ? "TIME" : $"TIME({unit}{utc})";
    }

    if(lt.TIMESTAMP != null) {
        TimestampType ts = lt.TIMESTAMP;
        string unit = ts.Unit.MILLIS != null ? "MILLIS" : ts.Unit.MICROS != null ? "MICROS" : ts.Unit.NANOS != null ? "NANOS" : string.Empty;
        string utc = ts.IsAdjustedToUTC ? ",UTC" : string.Empty;
        return string.IsNullOrEmpty(unit) ? "TIMESTAMP" : $"TIMESTAMP({unit}{utc})";
    }

    if(lt.INTEGER != null) {
        IntType i = lt.INTEGER;
        string sign = i.IsSigned ? "INT" : "UINT";
        return $"{sign}{i.BitWidth}";
    }

    if(lt.UNKNOWN != null)
        return "NULL";

    if(lt.JSON != null)
        return "JSON";

    if(lt.BSON != null)
        return "BSON";

    if(lt.VARIANT != null)
        return "VARIANT";

    return string.Empty;
}

void RenderOriginalSchema(List<SchemaElement> schemaElements) {
    Table("schema",
    ["name", "num children", "type", "type length", "repetition", "logical type", "converted type", "scale", "precision", "field id"],
    t => {
        foreach(SchemaElement se in schemaElements) {
            t.BeginRow();
            Label(se.Name);
            t.NextColumn();
            Label(se.NumChildren?.ToString() ?? "");
            t.NextColumn();
            Label(se.Type?.ToString() ?? "");
            t.NextColumn();
            Label(se.TypeLength?.ToString() ?? "");
            t.NextColumn();
            if(se.RepetitionType != null) {
                Emphasis emp = se.RepetitionType switch {
                    FieldRepetitionType.REQUIRED => Emphasis.Success,
                    FieldRepetitionType.OPTIONAL => Emphasis.Warning,
                    FieldRepetitionType.REPEATED => Emphasis.Info,
                    _ => Emphasis.None
                };
                Label(se.RepetitionType.ToString(), emp);
            }
            t.NextColumn();
            Label(FormatLogicalType(se.LogicalType));
            t.NextColumn();
            Label(se.ConvertedType?.ToString() ?? "");
            t.NextColumn();
            Label(se.Scale?.ToString() ?? "");
            t.NextColumn();
            Label(se.Precision?.ToString() ?? "");
            t.NextColumn();
            Label(se.FieldId?.ToString() ?? "");
        }
    }, 0, -20, true);
}

void Render(TableActions ta, IReadOnlyList<Field> fields) {
    foreach(Field f in fields) {
        ta.BeginRow();
        bool isLeaf = f.SchemaType == SchemaType.Data;
        TreeNode(f.Name, true, isLeaf, isOpen => {

            if(f is DataField df) {
                ta.NextColumn();
                Label(df.ClrType.ToString());
            } else {
                ta.NextColumn();
            }

            ta.NextColumn();
            Label(f.SchemaType.ToString());
            ta.NextColumn();
            bool n = f.IsNullable;
            Checkbox("##n", ref n);
            ta.NextColumn();
            Label(f.MaxRepetitionLevel.ToString());
            ta.NextColumn();
            Label(f.MaxDefinitionLevel.ToString());


            if(!isLeaf && isOpen) {
                switch(f.SchemaType) {
                    case SchemaType.List:
                        Render(ta, [((ListField)f).Item]);
                        break;
                    case SchemaType.Struct:
                        Render(ta, ((StructField)f).Fields);
                        break;
                }

            }
        });
    }
}

void RenderLogicalSchema(ParquetSchema schema) {
    Table("logicalSchema", ["Name", "CLR Type", "SchemaType", "IsNullable", "DL", "RL"], ta => {
        Render(ta, schema.Fields);
    }, 0, -20, true);
}

void RenderInfo() {

    Checkbox("raw", ref showRawSchema);

    if(showRawSchema) {
        RenderOriginalSchema(fd.Metadata!.Schema);
    } else {
        RenderLogicalSchema(fd.Schema);
    }
}

bool SBI(string icon, string? value, string? tooltip = null) {
    if(value == null)
        return false;

    SL();
    Label("|", isEnabled: false);
    SL();
    Label(icon);
    SL();
    Label(value);
    if(tooltip != null) {
        Tooltip(tooltip);
    }

    return true;
}

Run(title, () => {

    if(fd.ErrorMessage != null) {
        Label(fd.ErrorMessage, Emphasis.Error);
    }

    RenderInfo();

    StatusBar(() => {
        Label(fd.FileName);
        SBI(Icon.Attach_file, fd.SizeDisplay, "file size");
        SBI(Icon.Table_rows, fd.RowCountDisplay, "number of rows");
        SBI(Icon.Table_rows, fd.RowGroupCountDisplay, "number of row groups");
        SBI(Icon.View_column, fd.ColumnCountDisplay, "number of columns");
        SBI(Icon.Numbers, fd.VersionDisplay, "format version");
    });

    return true;
}, isScrollable: false);
