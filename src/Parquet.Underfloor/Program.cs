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

void RenderInfo() {

    if(!Accordion("Info"))
        return;

    LN(Icon.Create, "Created by", fd.Metadata?.CreatedBy);

    if(fd.RowGroups != null) {
        Sep("Row groups");
        int i = 0;
        foreach(IParquetRowGroupReader rg in fd.RowGroups) {
            RowGroup rg1 = rg.RowGroup;
            //string title = $"Row group {i++} ({rg.RowCount:N0} rows)";
            string title = $"Row group {++i}/{fd.RowGroups.Count}";
            if(Accordion(title)) {
                ALN("Rows", rg1.NumRows.ToString("N0"));
                ALN("File offset", rg1.FileOffset.ToString());
                ALN("Size uncompressed", rg1.TotalByteSize.ToFileSizeUiString());
                ALN("Size compressed", rg1.TotalCompressedSize?.ToFileSizeUiString());
            }
        }
    }

    //if(fd.Metadata?.KeyValueMetadata != null) {
    //    Sep("Key-value metadata");
    //    foreach(KeyValue kv in fd.Metadata.KeyValueMetadata) {
    //        string v = kv.Value ?? "";
    //        Input(ref v, kv.Key, true, 0, true);
    //    }
    //}

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

    //using(new TabBar()) {
    //    using(var ti = new TabItem("Info")) {
    //        if(ti) {
    //            RenderInfo();
    //        }
    //    }

    //    using(var ti = new TabItem("Data")) {
    //        if(ti) {
    //            RenderData();
    //        }
    //    }
    //}

    using(new StatusBar()) {
        Label(fd.FileName);
        SBI(Icon.Attach_file, fd.SizeDisplay, "file size");
        SBI(Icon.Table_rows, fd.RowCountDisplay, "number of rows");
        SBI(Icon.Table_rows, fd.RowGroupCountDisplay, "number of row groups");
        SBI(Icon.View_column, fd.ColumnCountDisplay, "number of columns");
        SBI(Icon.Numbers, fd.VersionDisplay, "format version");
    }

    return true;
}, isScrollable: false);
