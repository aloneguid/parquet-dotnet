using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ByteSizeLib;
using Parquet.Data;
using Parquet.Meta;
using Parquet.Schema;
using Parquet.Serialization;

namespace Parquet.Underfloor; 

static class X {
    public static string UISize(this long l) {
        return ByteSize.FromBytes(l).ToString();
    }
}

enum ReadStatus {
    NotStarted = 0,
    InProgress,
    Completed,
    Failed
}

/// <summary>
/// This keeps parquet file open for the duration of the session, in order to read parts of it on demand.
/// </summary>
class WorkFile : IAsyncDisposable {

    private Stream? _stream;
    private ParquetReader? _reader;
    private ParquetSchema? _schema;
    private FileMetaData? _meta;
    private DataField[]? _rawDataFields;

    public static async Task<WorkFile> CreateAsync(string? path) {
        var wf = new WorkFile();
        if(path == null) {
            wf.FilePath = "";
            wf.FileName = "?";
            wf.ErrorMessage = "No file path provided.";
        } else {
            wf.FilePath = path;
            wf.FileName = Path.GetFileName(path) ?? "?";
            wf.SizeDisplay = new FileInfo(path).Length.UISize();

            wf._stream = System.IO.File.OpenRead(path);
            wf._reader = await ParquetReader.CreateAsync(wf._stream);

            wf.Metadata = wf._reader.Metadata;
            wf.Schema = wf._reader.Schema;
            wf._rawDataFields = wf._reader.Schema.DataFields;
            wf.RawDataFieldsPaths = wf._rawDataFields.Select(df => string.Join('.', df.Path)).ToArray();
            wf.RowGroups = wf._reader.RowGroups;
            wf.RowGroupDisplayNames = wf.RowGroups?.Select((rg, i) => $"#{i + 1}").ToArray();
            wf.CurrentRowGroupIndex = 0;
        }

        return wf;
    }

    public string? ErrorMessage { get; set; }

    public string? FileName { get; set; }

    public string? FilePath { get; set; }

    public string? SizeDisplay { get; set; }

    public FileMetaData? Metadata {
        get => _meta;
        set {
            _meta = value;

            if(value != null) {
                RowCountDisplay = value.NumRows.ToString("N0");
                VersionDisplay = value.Version.ToString();
                RowGroupCountDisplay = value.RowGroups.Count.ToString("N0");
            }
        }
    }

    public string? RowCountDisplay { get; set; }

    public string? RowGroupCountDisplay { get; set; }

    public string? VersionDisplay { get; set; }

    public ParquetSchema? Schema {
        get => _schema;
        set {
            _schema = value;

            if(value != null) {
                ColumnCountDisplay = value.DataFields.Length.ToString("N0");
                Columns = value.Fields.Select(f => f.Name).ToArray();
                ColumnsDisplay = ["#", .. Columns];

            }
        }
    }

    public string? ColumnCountDisplay { get; set; }

    public string[]? Columns { get; set; }

    public string[]? ColumnsDisplay { get; set; }

    public IReadOnlyList<IParquetRowGroupReader>? RowGroups { get; set; }

    public string[]? RowGroupDisplayNames;

    public uint CurrentRowGroupIndex;

    public ReadStatus SampleReadStatus { get; set; } = ReadStatus.NotStarted;

    public ParquetSerializer.UntypedResult? Sample { get; set; }

    public Exception? SampleReadException { get; set; }

    public TimeSpan SampleReadDuration { get; set; }

    public string? SampleReadDurationDisplay { get; set; }

    public DataField[]? RawDataFields => _rawDataFields;

    public string[]? RawDataFieldsPaths;

    public uint CurrentRawDataFieldIndex = 0;

    public DataColumn? CurrentRawDataFieldData;

    public ReadStatus CurrentRawDataFieldDataReadStatus { get; set; } = ReadStatus.NotStarted;

    public Exception? CurrentRawDataFieldReadError = null;

    public async Task ReadDataSampleAsync() {
        if(_stream == null || SampleReadStatus == ReadStatus.InProgress)
            return;

        if(SampleReadStatus == ReadStatus.NotStarted) {
            SampleReadStatus = ReadStatus.InProgress;

            DateTime start = DateTime.UtcNow;
            _stream.Seek(0, SeekOrigin.Begin);
            try {
                ParquetSerializer.UntypedResult ur = await ParquetSerializer.DeserializeAsync(_stream,
                    new ParquetSerializerOptions {
                        ParquetOptions = new ParquetOptions {
                            TreatByteArrayAsString = true,
                            UseBigDecimal = true
                        }
                    });

                Sample = ur;
                SampleReadStatus = ReadStatus.Completed;
            } catch(Exception ex) {
                SampleReadException = ex;
                SampleReadStatus = ReadStatus.Failed;
            } finally {
                SampleReadDuration = DateTime.UtcNow - start;
                SampleReadDurationDisplay = SampleReadDuration.ToString();
            }
        }
    }

    public async Task ReadRawDataFieldAsync() {
        if(_stream == null || _rawDataFields == null)
            return;

        DataField df = _rawDataFields[CurrentRawDataFieldIndex];
        _stream.Seek(0, SeekOrigin.Begin);
        CurrentRawDataFieldData = null;
        CurrentRawDataFieldDataReadStatus = ReadStatus.InProgress;

        try {
            using ParquetReader pr = await ParquetReader.CreateAsync(_stream);
            using ParquetRowGroupReader rgr = pr.OpenRowGroupReader(0);
            DataColumn dc = await rgr.ReadColumnAsync(df);
            CurrentRawDataFieldData = dc;
            CurrentRawDataFieldDataReadStatus = ReadStatus.Completed;
        } catch(Exception ex) {
            CurrentRawDataFieldReadError = ex;
            CurrentRawDataFieldDataReadStatus = ReadStatus.Failed;
        }
    }

    public async ValueTask DisposeAsync() {
        if(_stream != null) {
            await _stream.DisposeAsync();
            _stream = null;
        }
    }
}
