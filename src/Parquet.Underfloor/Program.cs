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

#region [ Schema ]

bool showRawSchema = false;

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

void RenderRawSchema(List<SchemaElement> schemaElements) {
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

void RenderLogicalSchemaFields(TableActions ta, IReadOnlyList<Field> fields) {
    foreach(Field f in fields) {
        ta.BeginRow();
        bool isLeaf = f.SchemaType == SchemaType.Data;

        string name = f.SchemaType switch {
            SchemaType.List => Icon.Data_array,
            SchemaType.Map => Icon.Map,
            SchemaType.Struct => Icon.Data_object,
            _ => Icon.Square
        } + " " + f.Name;

        TreeNode(name, true, isLeaf, isOpen => {

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
            SmallCheckbox("##n", ref n);
            ta.NextColumn();
            Label(f.MaxRepetitionLevel.ToString());
            ta.NextColumn();
            Label(f.MaxDefinitionLevel.ToString());


            if(!isLeaf && isOpen) {
                switch(f.SchemaType) {
                    case SchemaType.List:
                        RenderLogicalSchemaFields(ta, [((ListField)f).Item]);
                        break;
                    case SchemaType.Struct:
                        RenderLogicalSchemaFields(ta, ((StructField)f).Fields);
                        break;
                    case SchemaType.Map:
                        MapField mf = (MapField)f;
                        RenderLogicalSchemaFields(ta, [mf.Key, mf.Value]);
                        break;
                }

            }
        }, true);
    }
}

void RenderLogicalSchema(ParquetSchema schema) {
    Table("logicalSchema", ["Name", "CLR Type", "SchemaType", "IsNullable", "DL", "RL"], ta => {
        RenderLogicalSchemaFields(ta, schema.Fields);
    }, 0, -20, true);
}

void RenderSchema() {

    if(fd.Schema == null)
        return;

    SmallCheckbox("raw schema", ref showRawSchema);

    if(showRawSchema) {
        RenderRawSchema(fd.Metadata!.Schema);
    } else {
        RenderLogicalSchema(fd.Schema);
    }
}

#endregion

#region [ Metadata ]

void RenderKeyValueMetadata(List<KeyValue>? kvm) {
    if(kvm == null || kvm.Count == 0)
        return;

    if(Accordion($"Metadata ({kvm.Count})")) {
        Table("kvm", ["key", "value"], ta => {

            foreach(KeyValue kv in kvm) {
                ta.BeginRow();
                Label(kv.Key);

                ta.NextColumn();
                if(Button($"{Icon.Content_copy}##{kv.Key}")) {
                    // copy to clipboard
                }
                SL();
                Label(kv.Value ?? "");
            }
        }, 0, -20, true);
    }
}

void RenderMetadata() {
    if(fd.Metadata == null)
        return;

    Label(Icon.Attribution, Emphasis.Primary);
    Tooltip("created by");
    SL();
    Label(fd.Metadata.CreatedBy ?? "");

    RenderKeyValueMetadata(fd.Metadata.KeyValueMetadata);

    // row grops and so on
    if(fd.RowGroups != null) {
        Table("rgs", ["Index/Path", "Row/value count", "File offset", "Size", "Compressed size", "Codec"], ta => {
            int idx = 0;
            foreach(IParquetRowGroupReader rg in fd.RowGroups) {
                ta.BeginRow();
                TreeNode(idx.ToString(), true, false, (bool isOpen) => {
                    ta.NextColumn();
                    Label(rg.RowGroup.NumRows.ToString("N0"));
                    ta.NextColumn();
                    Label(rg.RowGroup.FileOffset?.ToString() ?? "");
                    ta.NextColumn();
                    Label(rg.RowGroup.TotalByteSize.ToFileSizeUiString());
                    ta.NextColumn();
                    Label((rg.RowGroup.TotalCompressedSize ?? 0).ToFileSizeUiString());

                    if(isOpen) {
                        int idx1 = 0;
                        foreach(ColumnChunk cc in rg.RowGroup.Columns) {

                            ta.BeginRow();
                            if(cc.MetaData != null) {
                                Label(string.Join(".", cc.MetaData.PathInSchema));
                            }
                            ta.NextColumn();
                            Label(cc.MetaData?.NumValues.ToString() ?? "");
                            ta.NextColumn();
                            Label(cc.FileOffset == 0 ? "" : cc.FileOffset.ToString());
                            ta.NextColumn();
                            Label(cc.MetaData?.TotalUncompressedSize.ToFileSizeUiString() ?? "");
                            ta.NextColumn();
                            Label(cc.MetaData?.TotalCompressedSize.ToFileSizeUiString() ?? "");
                            ta.NextColumn();
                            Label(cc.MetaData?.Codec.ToString() ?? "");

                            idx1++;    
                        }

                    }

                    idx++;
                }, true);
            }
        }, 0, -20, true);
    }
}

#endregion

#region [ Status Bar ]
void RenderStatusBar() {

    void SBI(string icon, string? value, string? tooltip = null) {
        if(value == null)
            return;

        SL();
        Label("|", isEnabled: false);
        SL();
        Label(icon);
        SL();
        Label(value);
        if(tooltip != null) {
            Tooltip(tooltip);
        }
    }

    if(fd.FileName != null) {
        Label(fd.FileName);
    }

    SBI(Icon.Attach_file, fd.SizeDisplay, "file size");
    SBI(Icon.Table_rows, fd.RowCountDisplay, "number of rows");
    SBI(Icon.Table_rows, fd.RowGroupCountDisplay, "number of row groups");
    SBI(Icon.View_column, fd.ColumnCountDisplay, "number of columns");
    SBI(Icon.Numbers, fd.VersionDisplay, "format version");
}

#endregion

#region [ Data ]

void RenderValue(int row, int col, Field f, object? value) {
    if(value is null) {
        Label("NULL", isEnabled: false);
    } else {
        switch(f.SchemaType) {
            case SchemaType.Data:
                Label(value.ToString() ?? "");
                break;
            case SchemaType.List:
                if(Button(Icon.Data_array, isSmall: true)) {
                    // todo
                }
                break;
            case SchemaType.Map:
                if(Button(Icon.Map, isSmall: true)) {
                    // todo
                }
                break;
            case SchemaType.Struct:
                if(Button(Icon.Data_object, isSmall: true)) {
                    // todo
                }
                break;
            default:
                Label("N/A", Emphasis.Warning);
                break;
        }
    }
}

void RenderData() {
    if(fd.Metadata == null || fd.Columns == null || fd.ColumnsDisplay == null || fd.Schema == null)
        return;

    Label(fd.SampleReadStatus.ToString(), Emphasis.Info);

    if(fd.SampleReadStatus == ReadStatus.NotStarted) {
        fd.ReadDataSampleAsync().Forget();
        return;
    }

    if(fd.SampleReadException != null) {
        Label("Error reading data sample", Emphasis.Error);
        Label(fd.SampleReadException.ToString());
        return;
    }

    BigTable("data", fd.ColumnsDisplay, (int)fd.Metadata.NumRows,
        (int row, int col) => {

            if(col == 0) {
                Selectable(row.ToString(), spanColumns: true);
                return;
            }

            if(fd.Sample == null || fd.Sample.Data.Count() < row)
                return;

            Dictionary<string, object> cell = fd.Sample.Data[row];
            string colName = fd.ColumnsDisplay[col];
            Field f = fd.Schema[col - 1];
            cell.TryGetValue(colName, out object? value);
            RenderValue(row, col, f, value);
        },
        0, -20, true);

}

#endregion

Run(title, () => {

    if(fd.ErrorMessage != null) {
        Label(fd.ErrorMessage, Emphasis.Error);
    }

    TabBar("top", tba => {
        tba.TabItem("Schema", RenderSchema);
        tba.TabItem("Metadata", RenderMetadata);
        tba.TabItem("Data", RenderData);
    });

    StatusBar(RenderStatusBar);

    return true;
}, isScrollable: false);
