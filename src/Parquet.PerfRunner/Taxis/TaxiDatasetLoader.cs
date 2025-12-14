using Parquet.Schema;
using IOFile = System.IO.File;
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
            using ParquetReaderNet reader = await ParquetReaderNet.CreateAsync(fs);

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

                vendorIds.AddRange((int?[])(await rg.ReadColumnAsync(vendor)).Data);
                pickupTimes.AddRange((DateTime?[])(await rg.ReadColumnAsync(pickup)).Data);
                dropoffTimes.AddRange((DateTime?[])(await rg.ReadColumnAsync(dropoff)).Data);
                passengerCounts.AddRange((long?[])(await rg.ReadColumnAsync(passengerCount)).Data);
                tripDistances.AddRange((double?[])(await rg.ReadColumnAsync(tripDistance)).Data);
                rateCodes.AddRange((long?[])(await rg.ReadColumnAsync(rateCode)).Data);
                storeAndFwdFlags.AddRange((string?[])(await rg.ReadColumnAsync(storeAndFwdFlag)).Data);
                puLocationIds.AddRange((int?[])(await rg.ReadColumnAsync(puLocationId)).Data);
                doLocationIds.AddRange((int?[])(await rg.ReadColumnAsync(doLocationId)).Data);
                paymentTypes.AddRange((long?[])(await rg.ReadColumnAsync(paymentType)).Data);
                fareAmounts.AddRange((double?[])(await rg.ReadColumnAsync(fareAmount)).Data);
                extras.AddRange((double?[])(await rg.ReadColumnAsync(extra)).Data);
                mtaTaxes.AddRange((double?[])(await rg.ReadColumnAsync(mtaTax)).Data);
                tipAmounts.AddRange((double?[])(await rg.ReadColumnAsync(tipAmount)).Data);
                tollsAmounts.AddRange((double?[])(await rg.ReadColumnAsync(tollsAmount)).Data);
                improvementSurcharges.AddRange((double?[])(await rg.ReadColumnAsync(improvementSurcharge)).Data);
                totalAmounts.AddRange((double?[])(await rg.ReadColumnAsync(totalAmount)).Data);
                congestionSurcharges.AddRange((double?[])(await rg.ReadColumnAsync(congestionSurcharge)).Data);
                airportFees.AddRange((double?[])(await rg.ReadColumnAsync(airportFee)).Data);

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
