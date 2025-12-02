using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using BenchmarkDotNet.Attributes;
using Parquet.Data;
using Parquet.Schema;
using ParquetSharp;
using ParquetSharp.IO;
using Column = ParquetSharp.Column;
using ParquetReaderNet = Parquet.ParquetReader;
using ParquetWriterNet = Parquet.ParquetWriter;
using IOFile = System.IO.File;

namespace Parquet.PerfRunner.Benchmarks;

[MemoryDiagnoser]
[MarkdownExporter]
[ShortRunJob]
public class TaxiCsvToParquetBenchmark
{
    // Official NYC TLC parquet. The "tripdata" dataset is a single month; "tripdata-large" combines multiple months.
    private const string BaseUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/";
    private static readonly IReadOnlyDictionary<string, string[]> DatasetFiles = new Dictionary<string, string[]>
    {
        { "tripdata", new[] { "yellow_tripdata_2024-01.parquet" } },
        { "tripdata-large", new[] { "yellow_tripdata_2024-01.parquet", "yellow_tripdata_2024-02.parquet", "yellow_tripdata_2024-03.parquet" } }
    };

    private int?[]? _vendorIds;
    private int?[]? _rateCodes;
    private double?[]? _passengerCounts;
    private double?[]? _tripDistances;
    private int?[]? _paymentTypes;
    private double?[]? _fareAmounts;

    private ParquetSchema? _parquetSchema;
    private DataColumn[]? _parquetNetColumns;
    private Column[]? _parquetSharpColumns;

    [Params("tripdata", "tripdata-large")]
    public string Dataset { get; set; } = "tripdata";

    [GlobalSetup]
    public async Task LoadDataset()
    {
        string[] parquetPaths = await EnsureDatasetsAsync();
        TaxiColumns columns = await LoadParquetColumnsAsync(parquetPaths);

        _vendorIds = columns.VendorIds;
        _rateCodes = columns.RateCodes;
        _passengerCounts = columns.PassengerCounts;
        _tripDistances = columns.TripDistances;
        _paymentTypes = columns.PaymentTypes;
        _fareAmounts = columns.FareAmounts;

        _parquetSchema = new ParquetSchema(
            new DataField<int?>("vendorid"),
            new DataField<int?>("ratecodeid"),
            new DataField<double?>("passenger_count"),
            new DataField<double?>("trip_distance"),
            new DataField<int?>("payment_type"),
            new DataField<double?>("fare_amount"));

        DataField[] fields = _parquetSchema.DataFields;
        _parquetNetColumns =
        [
            new DataColumn(fields[0], _vendorIds),
            new DataColumn(fields[1], _rateCodes),
            new DataColumn(fields[2], _passengerCounts),
            new DataColumn(fields[3], _tripDistances),
            new DataColumn(fields[4], _paymentTypes),
            new DataColumn(fields[5], _fareAmounts)
        ];

        _parquetSharpColumns =
        [
            new Column<int?>("vendorid"),
            new Column<int?>("ratecodeid"),
            new Column<double?>("passenger_count"),
            new Column<double?>("trip_distance"),
            new Column<int?>("payment_type"),
            new Column<double?>("fare_amount")
        ];
    }

    [Benchmark(Description = "Parquet.Net -> MemoryStream")]
    public async Task ParquetNet()
    {
        using var output = new MemoryStream();
        using ParquetWriterNet writer = await ParquetWriterNet.CreateAsync(_parquetSchema!, output);
        using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();

        foreach (DataColumn column in _parquetNetColumns!)
        {
            await rowGroup.WriteColumnAsync(column);
        }
    }

    [Benchmark(Description = "ParquetSharp -> MemoryStream")]
    public void ParquetSharp()
    {
        using var output = new MemoryStream();
        using var managedOutput = new ManagedOutputStream(output);
        using var writer = new ParquetFileWriter(managedOutput, _parquetSharpColumns!);
        using RowGroupWriter rowGroup = writer.AppendRowGroup();

        using (LogicalColumnWriter<int?> vendorWriter = rowGroup.NextColumn().LogicalWriter<int?>())
        {
            vendorWriter.WriteBatch(_vendorIds!);
        }

        using (LogicalColumnWriter<int?> rateCodeWriter = rowGroup.NextColumn().LogicalWriter<int?>())
        {
            rateCodeWriter.WriteBatch(_rateCodes!);
        }

        using (LogicalColumnWriter<double?> passengerCountWriter = rowGroup.NextColumn().LogicalWriter<double?>())
        {
            passengerCountWriter.WriteBatch(_passengerCounts!);
        }

        using (LogicalColumnWriter<double?> tripDistanceWriter = rowGroup.NextColumn().LogicalWriter<double?>())
        {
            tripDistanceWriter.WriteBatch(_tripDistances!);
        }

        using (LogicalColumnWriter<int?> paymentTypeWriter = rowGroup.NextColumn().LogicalWriter<int?>())
        {
            paymentTypeWriter.WriteBatch(_paymentTypes!);
        }

        using (LogicalColumnWriter<double?> fareWriter = rowGroup.NextColumn().LogicalWriter<double?>())
        {
            fareWriter.WriteBatch(_fareAmounts!);
        }

        writer.Close();
    }

    [Benchmark(Description = "Parquet.Net -> Disk")]
    public async Task ParquetNetToDisk()
    {
        string path = Path.Combine(Path.GetTempPath(), $"taxi-parquetnet-{Guid.NewGuid():N}.parquet");
        try
        {
            await using FileStream output = IOFile.Create(path);
            using ParquetWriterNet writer = await ParquetWriterNet.CreateAsync(_parquetSchema!, output);
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();

            foreach (DataColumn column in _parquetNetColumns!)
            {
                await rowGroup.WriteColumnAsync(column);
            }
        }
        finally
        {
            TryDelete(path);
        }
    }

    [Benchmark(Description = "ParquetSharp -> Disk")]
    public void ParquetSharpToDisk()
    {
        string path = Path.Combine(Path.GetTempPath(), $"taxi-parquetsharp-{Guid.NewGuid():N}.parquet");
        try
        {
            using FileStream output = IOFile.Create(path);
            using var managedOutput = new ManagedOutputStream(output);
            using var writer = new ParquetFileWriter(managedOutput, _parquetSharpColumns!);
            using RowGroupWriter rowGroup = writer.AppendRowGroup();

            using (LogicalColumnWriter<int?> vendorWriter = rowGroup.NextColumn().LogicalWriter<int?>())
            {
                vendorWriter.WriteBatch(_vendorIds!);
            }

            using (LogicalColumnWriter<int?> rateCodeWriter = rowGroup.NextColumn().LogicalWriter<int?>())
            {
                rateCodeWriter.WriteBatch(_rateCodes!);
            }

            using (LogicalColumnWriter<double?> passengerCountWriter = rowGroup.NextColumn().LogicalWriter<double?>())
            {
                passengerCountWriter.WriteBatch(_passengerCounts!);
            }

            using (LogicalColumnWriter<double?> tripDistanceWriter = rowGroup.NextColumn().LogicalWriter<double?>())
            {
                tripDistanceWriter.WriteBatch(_tripDistances!);
            }

            using (LogicalColumnWriter<int?> paymentTypeWriter = rowGroup.NextColumn().LogicalWriter<int?>())
            {
                paymentTypeWriter.WriteBatch(_paymentTypes!);
            }

            using (LogicalColumnWriter<double?> fareWriter = rowGroup.NextColumn().LogicalWriter<double?>())
            {
                fareWriter.WriteBatch(_fareAmounts!);
            }

            writer.Close();
        }
        finally
        {
            TryDelete(path);
        }
    }

    private static string GetDataDirectory()
    {
        return Path.Combine(AppContext.BaseDirectory, "Data");
    }

    private static string? TryProjectDataPath(string fileName)
    {
        string projectPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../Data", fileName));
        return IOFile.Exists(projectPath) ? projectPath : null;
    }

    private async Task<string[]> EnsureDatasetsAsync()
    {
        if (!DatasetFiles.TryGetValue(Dataset, out string[]? files))
        {
            throw new ArgumentOutOfRangeException(nameof(Dataset), Dataset, "Unknown dataset alias");
        }

        string dataDir = GetDataDirectory();
        Directory.CreateDirectory(dataDir);

        var paths = new List<string>(files.Length);
        using var httpClient = new HttpClient();

        foreach (string file in files)
        {
            string dataPath = Path.Combine(dataDir, file);
            if (IOFile.Exists(dataPath))
            {
                paths.Add(dataPath);
                continue;
            }

            string? projectPath = TryProjectDataPath(file);
            if (projectPath != null)
            {
                paths.Add(projectPath);
                continue;
            }

            string url = $"{BaseUrl}{file}";
            byte[] bytes = await httpClient.GetByteArrayAsync(url);
            await IOFile.WriteAllBytesAsync(dataPath, bytes);
            paths.Add(dataPath);
        }

        return paths.ToArray();
    }

    private static async Task<TaxiColumns> LoadParquetColumnsAsync(IEnumerable<string> paths)
    {
        var vendorIds = new List<int?>();
        var rateCodes = new List<int?>();
        var passengerCounts = new List<double?>();
        var tripDistances = new List<double?>();
        var paymentTypes = new List<int?>();
        var fareAmounts = new List<double?>();

        foreach (string path in paths)
        {
            using FileStream fs = IOFile.OpenRead(path);
            using ParquetReaderNet reader = await ParquetReaderNet.CreateAsync(fs);

            DataField vendor = FindField(reader.Schema, "vendorid");
            DataField rateCode = FindField(reader.Schema, "ratecodeid");
            DataField passengerCount = FindField(reader.Schema, "passenger_count");
            DataField tripDistance = FindField(reader.Schema, "trip_distance");
            DataField paymentType = FindField(reader.Schema, "payment_type");
            DataField fareAmount = FindField(reader.Schema, "fare_amount");

            for (int i = 0; i < reader.RowGroupCount; i++)
            {
                using ParquetRowGroupReader rg = reader.OpenRowGroupReader(i);

                vendorIds.AddRange(ToNullableInt((await rg.ReadColumnAsync(vendor)).Data));
                rateCodes.AddRange(ToNullableInt((await rg.ReadColumnAsync(rateCode)).Data));
                passengerCounts.AddRange(ToNullableDouble((await rg.ReadColumnAsync(passengerCount)).Data));
                tripDistances.AddRange(ToNullableDouble((await rg.ReadColumnAsync(tripDistance)).Data));
                paymentTypes.AddRange(ToNullableInt((await rg.ReadColumnAsync(paymentType)).Data));
                fareAmounts.AddRange(ToNullableDouble((await rg.ReadColumnAsync(fareAmount)).Data));
            }
        }

        return new TaxiColumns(
            vendorIds.ToArray(),
            rateCodes.ToArray(),
            passengerCounts.ToArray(),
            tripDistances.ToArray(),
            paymentTypes.ToArray(),
            fareAmounts.ToArray());
    }

    private static DataField FindField(ParquetSchema schema, string name)
    {
        return schema.DataFields.First(f => string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase));
    }

    private static IReadOnlyList<int?> ToNullableInt(Array data) =>
        data switch
        {
            int?[] v => v,
            int[] v => v.Select(i => (int?)i).ToArray(),
            long?[] v => v.Select(i => i.HasValue ? (int?)checked((int)i.Value) : null).ToArray(),
            long[] v => v.Select(i => (int?)checked((int)i)).ToArray(),
            double?[] v => v.Select(i => i.HasValue ? (int?)checked((int)i.Value) : null).ToArray(),
            double[] v => v.Select(i => (int?)checked((int)i)).ToArray(),
            float?[] v => v.Select(i => i.HasValue ? (int?)checked((int)i.Value) : null).ToArray(),
            float[] v => v.Select(i => (int?)checked((int)i)).ToArray(),
            _ => throw new InvalidOperationException($"Unsupported numeric type: {data.GetType()}")
        };

    private static IReadOnlyList<double?> ToNullableDouble(Array data) =>
        data switch
        {
            double?[] v => v,
            double[] v => v.Select(i => (double?)i).ToArray(),
            float?[] v => v.Select(i => i.HasValue ? (double?)i.Value : null).ToArray(),
            float[] v => v.Select(i => (double?)i).ToArray(),
            int?[] v => v.Select(i => i.HasValue ? (double?)i.Value : null).ToArray(),
            int[] v => v.Select(i => (double?)i).ToArray(),
            long?[] v => v.Select(i => i.HasValue ? (double?)i.Value : null).ToArray(),
            long[] v => v.Select(i => (double?)i).ToArray(),
            _ => throw new InvalidOperationException($"Unsupported numeric type: {data.GetType()}")
        };

    private static void TryDelete(string path)
    {
        try
        {
            if (IOFile.Exists(path))
            {
                IOFile.Delete(path);
            }
        }
        catch
        {
            // best effort cleanup
        }
    }
}

internal readonly struct TaxiColumns
{
    public TaxiColumns(int?[] vendorIds, int?[] rateCodes, double?[] passengerCounts, double?[] tripDistances, int?[] paymentTypes, double?[] fareAmounts)
    {
        VendorIds = vendorIds;
        RateCodes = rateCodes;
        PassengerCounts = passengerCounts;
        TripDistances = tripDistances;
        PaymentTypes = paymentTypes;
        FareAmounts = fareAmounts;
    }

    public int?[] VendorIds { get; }
    public int?[] RateCodes { get; }
    public double?[] PassengerCounts { get; }
    public double?[] TripDistances { get; }
    public int?[] PaymentTypes { get; }
    public double?[] FareAmounts { get; }
}
