using System.IO;
using Grey;
using Parquet;
using Parquet.Serialization;
using static Grey.App;

// global state
string? fileName = null;
bool isLoading = false;
bool hasLoaded = false;
ParquetSerializer.UntypedResult? parquetData = null;
string[]? columns = null;
string? errorMessage = null;
string fileSize = "";
string rowCount = "";

async Task LoadFromFileAsync(string path) {
    isLoading = true;
    try {
        fileSize = new FileInfo(path).Length.ToFileSizeUiString();

        using(Stream fileStream = File.OpenRead(path)) {
            parquetData = await ParquetSerializer.DeserializeAsync(
                    fileStream,
                    new ParquetSerializerOptions {
                        ParquetOptions = new ParquetOptions {
                            TreatByteArrayAsString = true
                        }
                    });
        }

        columns = parquetData.Schema.Fields.Select(f => f.Name).ToArray();
        rowCount = $"{parquetData.Data.Count} rows";

        hasLoaded = true;
    } catch(Exception ex) {
        errorMessage = ex.Message;
    } finally {
        isLoading = false;
    }
}

void LoadFromFile(string path) {
    LoadFromFileAsync(path).Forget();
}

if(args.Length == 0) {
    // no file specified, show error message
    errorMessage = "Parquet file path needs to be passed as first argument.";
} else if(args.Length == 1) {
    fileName = args[0];

    if(File.Exists(fileName)) {
        // file exists, load it
        LoadFromFile(fileName);
    } else {
        // file does not exist, show error message
        errorMessage = $"File does not exist.";
    }
}

string title = "Parquet Underfloor";

if(!string.IsNullOrEmpty(fileName)) {
    title += $" - {Path.GetFileName(fileName)}";
}

void RenderData() {
    if(parquetData == null || columns == null) {
        Label("No data loaded.");
        return;
    }

    Table("data", columns, parquetData.Data.Count, (int rowIdx, int colIdx) => {
        Dictionary<string, object> row = parquetData.Data[rowIdx];
        string columnName = columns[colIdx];
        if(row.TryGetValue(columnName, out object? value)) {
            if(value == null) {
                Button("null", isEnabled: false, isSmall: true);
            } else {
                Label(value.ToString());
            }
        }
    });
}

Run(title, () => {

    if(errorMessage != null) {
        Label(errorMessage, Emphasis.Error);
    }

    if(isLoading) {
        Label("loading...");
    }

    if(hasLoaded) {

        using(new TabBar()) {
            using(var ti = new TabItem("Data")) {
                if(ti) {
                    RenderData();
                }
            }
        }

        using(new StatusBar()) {
            Label(fileSize);

            if(rowCount != null) {
                SL(); Label("|", Emphasis.None, 0, false);
                SL(); Label(rowCount);
            }
        }
    }

    return true;
}, isScrollable: false);
