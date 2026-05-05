using Parquet.Schema;
using IOFile = System.IO.File;
using Parquet.Data;
using ParquetReaderNet = Parquet.ParquetReader;

namespace Parquet.PerfRunner.Taxis;

class TaxiDatasetLoader {

    public static TaxiDatasetLoader Instance { get; } = new TaxiDatasetLoader();
    TaxiDatasetLoader() {}


    // Official NYC TLC parquet. The "tripdata" dataset is a single month; "tripdata-large" combines multiple months.
    private const string _baseUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/";
    private static readonly HttpClient _httpClient = new();

    private static readonly IReadOnlyDictionary<string, string[]> DatasetFiles = new Dictionary<string, string[]>
    {
        { "tripdata", new[] { "yellow_tripdata_2024-01.parquet" } },
        { "tripdata-large", new[] { "yellow_tripdata_2024-01.parquet", "yellow_tripdata_2024-02.parquet", "yellow_tripdata_2024-03.parquet" } }
    };

    static readonly string _dataFolder = Path.Combine(Path.GetTempPath(), "nyc_tlc_data");

    public async Task<TaxiDataset> LoadAsync(string datasetName) {
        Directory.CreateDirectory(_dataFolder);

        if(!DatasetFiles.TryGetValue(datasetName, out string[]? files)) {
            throw new ArgumentException($"Unknown dataset: {datasetName}", nameof(datasetName));
        }

        await PreloadFiles(files);
        TaxiDataset dataset = await LoadParquetColumnsAsync(files);
        return dataset;
    }

    private static async Task<TaxiDataset> LoadParquetColumnsAsync(IEnumerable<string> paths) {
        List<int?> vendorIds = [];
        List<DateTime?> pickupTimes = [];
        List<DateTime?> dropoffTimes = [];
        List<long?> passengerCounts = [];
        List<double?> tripDistances = [];
        List<long?> rateCodes = [];
        List<string?> storeAndFwdFlags = [];
        List<int?> puLocationIds = [];
        List<int?> doLocationIds = [];
        List<long?> paymentTypes = [];
        List<double?> fareAmounts = [];
        List<double?> extras = [];
        List<double?> mtaTaxes = [];
        List<double?> tipAmounts = [];
        List<double?> tollsAmounts = [];
        List<double?> improvementSurcharges = [];
        List<double?> totalAmounts = [];
        List<double?> congestionSurcharges = [];
        List<double?> airportFees = [];

        foreach(string path in paths) {
            using FileStream fs = IOFile.OpenRead(Path.Combine(_dataFolder, path));
            await using ParquetReaderNet reader = await ParquetReaderNet.CreateAsync(fs);

            DataField vendor = FindDataField(reader.Schema, "VendorID");
            DataField pickup = FindDataField(reader.Schema, "tpep_pickup_datetime");
            DataField dropoff = FindDataField(reader.Schema, "tpep_dropoff_datetime");
            DataField passengerCount = FindDataField(reader.Schema, "passenger_count");
            DataField tripDistance = FindDataField(reader.Schema, "trip_distance");
            DataField rateCode = FindDataField(reader.Schema, "RatecodeID");
            DataField storeAndFwdFlag = FindDataField(reader.Schema, "store_and_fwd_flag");
            DataField puLocationId = FindDataField(reader.Schema, "PULocationID");
            DataField doLocationId = FindDataField(reader.Schema, "DOLocationID");
            DataField paymentType = FindDataField(reader.Schema, "payment_type");
            DataField fareAmount = FindDataField(reader.Schema, "fare_amount");
            DataField extra = FindDataField(reader.Schema, "extra");
            DataField mtaTax = FindDataField(reader.Schema, "mta_tax");
            DataField tipAmount = FindDataField(reader.Schema, "tip_amount");
            DataField tollsAmount = FindDataField(reader.Schema, "tolls_amount");
            DataField improvementSurcharge = FindDataField(reader.Schema, "improvement_surcharge");
            DataField totalAmount = FindDataField(reader.Schema, "total_amount");
            DataField congestionSurcharge = FindDataField(reader.Schema, "congestion_surcharge");
            DataField airportFee = FindDataField(reader.Schema, "Airport_fee");

            for(int i = 0; i < reader.RowGroupCount; i++) {
                using ParquetRowGroupReader rg = reader.OpenRowGroupReader(i);

                await AddNullableValuesAsync<int>(rg, vendor, vendorIds);
                await AddNullableValuesAsync<DateTime>(rg, pickup, pickupTimes);
                await AddNullableValuesAsync<DateTime>(rg, dropoff, dropoffTimes);
                await AddNullableValuesAsync<long>(rg, passengerCount, passengerCounts);
                await AddNullableValuesAsync<double>(rg, tripDistance, tripDistances);
                await AddNullableValuesAsync<long>(rg, rateCode, rateCodes);
                await AddNullableStringsAsync(rg, storeAndFwdFlag, storeAndFwdFlags);
                await AddNullableValuesAsync<int>(rg, puLocationId, puLocationIds);
                await AddNullableValuesAsync<int>(rg, doLocationId, doLocationIds);
                await AddNullableValuesAsync<long>(rg, paymentType, paymentTypes);
                await AddNullableValuesAsync<double>(rg, fareAmount, fareAmounts);
                await AddNullableValuesAsync<double>(rg, extra, extras);
                await AddNullableValuesAsync<double>(rg, mtaTax, mtaTaxes);
                await AddNullableValuesAsync<double>(rg, tipAmount, tipAmounts);
                await AddNullableValuesAsync<double>(rg, tollsAmount, tollsAmounts);
                await AddNullableValuesAsync<double>(rg, improvementSurcharge, improvementSurcharges);
                await AddNullableValuesAsync<double>(rg, totalAmount, totalAmounts);
                await AddNullableValuesAsync<double>(rg, congestionSurcharge, congestionSurcharges);
                await AddNullableValuesAsync<double>(rg, airportFee, airportFees);

            }
        }

        return new TaxiDataset(
            vendorIds.ToArray(),
            pickupTimes.ToArray(),
            dropoffTimes.ToArray(),
            passengerCounts.ToArray(),
            tripDistances.ToArray(),
            rateCodes.ToArray(),
            storeAndFwdFlags.ToArray(),
            puLocationIds.ToArray(),
            doLocationIds.ToArray(),
            paymentTypes.ToArray(),
            fareAmounts.ToArray(),
            extras.ToArray(),
            mtaTaxes.ToArray(),
            tipAmounts.ToArray(),
            tollsAmounts.ToArray(),
            improvementSurcharges.ToArray(),
            totalAmounts.ToArray(),
            congestionSurcharges.ToArray(),
            airportFees.ToArray()
        );
    }

    private static DataField FindDataField(ParquetSchema schema, string v) => schema.GetDataFields().First(f => f.Name == v);

    private static async Task AddNullableValuesAsync<T>(
        ParquetRowGroupReader rowGroup,
        DataField field,
        ICollection<T?> destination) where T : struct {
        using RawColumnData rawData = await rowGroup.ReadRawColumnDataBaseAsync(field);
        RawColumnData<T> data = (RawColumnData<T>)rawData;

        if(field.MaxDefinitionLevel == 0) {
            foreach(T value in data.Values) {
                destination.Add(value);
            }
            return;
        }

        int valueIndex = 0;
        foreach(int definitionLevel in data.DefinitionLevels) {
            destination.Add(definitionLevel == field.MaxDefinitionLevel ? data.Values[valueIndex++] : null);
        }
    }

    private static async Task AddNullableStringsAsync(
        ParquetRowGroupReader rowGroup,
        DataField field,
        ICollection<string?> destination) {
        using RawColumnData rawData = await rowGroup.ReadRawColumnDataBaseAsync(field);
        RawColumnData<ReadOnlyMemory<char>> data = (RawColumnData<ReadOnlyMemory<char>>)rawData;

        if(field.MaxDefinitionLevel == 0) {
            foreach(ReadOnlyMemory<char> value in data.Values) {
                destination.Add(new string(value.Span));
            }
            return;
        }

        int valueIndex = 0;
        foreach(int definitionLevel in data.DefinitionLevels) {
            destination.Add(definitionLevel == field.MaxDefinitionLevel ? new string(data.Values[valueIndex++].Span) : null);
        }
    }

    private static async Task PreloadFiles(IEnumerable<string> files)
        => await Task.WhenAll(
            files.Select(DownloadFileAsync)
        );

    private static async Task DownloadFileAsync(string fileName) {
        string dataPath = Path.Combine(_dataFolder, fileName);
        if(IOFile.Exists(dataPath)) {
            return;
        }
        string url = $"{_baseUrl}{fileName}";
        using Stream stream = await _httpClient.GetStreamAsync(url);
        using FileStream fileStream = IOFile.Create(dataPath);
        await stream.CopyToAsync(fileStream);
    }
}
