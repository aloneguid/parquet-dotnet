using Grey;
using Parquet;
using Parquet.Meta;
using Parquet.Schema;
using Parquet.Serialization;
using Parquet.Underfloor;
using static Grey.App;

// global state
FileData fd = new FileData(null);

async Task LoadSchemaAsync() {
    using ParquetReader reader = await ParquetReader.CreateAsync(fd.FilePath);
    fd.Schema = reader.Schema;
    fd.CustomMetadata = reader.CustomMetadata;
    fd.FileMeta = reader.Metadata;
    fd.RowGroups = reader.RowGroups;
    
    fd.RowCountDisplay = reader.Metadata?.NumRows.ToString("N0");
    fd.RowGroupCountDisplay = reader.RowGroupCount.ToString("N0");
    fd.VersionDisplay = reader.Metadata?.Version.ToString();
    fd.ColumnCountDisplay = reader.Schema.Fields.Count.ToString("N0");

    fd.Columns = reader.Schema.Fields.Select(f => f.Name).ToArray();
}

async Task LoadFromFileAsync(string path) {

    fd = new FileData(path);

    try {
        await LoadSchemaAsync();
    } catch(Exception ex) {
        fd.ErrorMessage = ex.Message;
        return;
    }

    fd.IsDataLoading = true;
    try {
        using(Stream fileStream = File.OpenRead(path)) {
            fd.Data = await ParquetSerializer.DeserializeAsync(
                    fileStream,
                    new ParquetSerializerOptions {
                        ParquetOptions = new ParquetOptions {
                            TreatByteArrayAsString = true
                        }
                    });
        }

        fd.HasLoaded = true;
    } catch(Exception ex) {
        fd.ErrorMessage = ex.Message;
    } finally {
        fd.IsDataLoading = false;
    }
}

void LoadFromFile(string path) {
    LoadFromFileAsync(path).Forget();
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

//if(!string.IsNullOrEmpty(fileName)) {
//    title += $" - {Path.GetFileName(fileName)}";
//}

void RenderPrimitiveValue(int rowIdx, int colIdx, DataField df, object value) {
    if(df.ClrType == typeof(bool)) {
        bool b = (bool)value;
        Checkbox($"##{rowIdx}-{colIdx}", ref b);
        return;
    }

    Label(value?.ToString() ?? "");
}

void RenderValue(int rowIdx, int colIdx, Field f, object? value) {
    if(value == null) {
        Button($"null##{rowIdx}-{colIdx}", isEnabled: false, isSmall: true);
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

    } else {
        Label(f.SchemaType.ToString());
    }
}

void RenderData() {

    if(fd.Columns == null)
        return;

    int rowCount = fd.Data?.Data.Count ?? 0;

    Table("data", fd.Columns, rowCount, (int rowIdx, int colIdx) => {
        if(fd.Data == null)
            return;

        Dictionary<string, object> row = fd.Data.Data[rowIdx];
        string columnName = fd.Columns[colIdx];
        Field f = fd.Data.Schema[colIdx];
        row.TryGetValue(columnName, out object? value);
        RenderValue(rowIdx, colIdx, f, value);
    });
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

    LN(Icon.Create, "Created by", fd.FileMeta?.CreatedBy);
   
    if(fd.RowGroups != null) {
        Sep("Row groups");
        int i = 0;
        foreach(IParquetRowGroupReader rg in fd.RowGroups) {
            RowGroup rg1 = rg.RowGroup;
            string title = $"Row group {i++} ({rg.RowCount} rows)";
            if(Accordion(title)) {
                ALN("Rows", rg1.NumRows.ToString("N0"));
                ALN("File offset", rg1.FileOffset.ToString());
                ALN("Size uncompressed", rg1.TotalByteSize.ToFileSizeUiString());
                ALN("Size compressed", rg1.TotalCompressedSize?.ToFileSizeUiString());
            }
        }
    }

    if(fd.FileMeta?.KeyValueMetadata != null) {
        Sep("Key-value metadata");
        foreach(KeyValue kv in fd.FileMeta.KeyValueMetadata) {
            string v = kv.Value ?? "";
            Input(ref v, kv.Key, true, 0, true);
        }
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

  

    using(new TabBar()) {

        using(var ti = new TabItem("Data")) {
            if(ti) {
                RenderData();
            }
        }

        using(var ti = new TabItem("Info")) {
            if(ti) {
                RenderInfo();
            }
        }
    }

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
