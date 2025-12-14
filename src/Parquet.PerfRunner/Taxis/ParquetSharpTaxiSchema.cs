using ParquetSharp;
using Column = ParquetSharp.Column;

namespace Parquet.PerfRunner.Taxis;

sealed record ParquetSharpTaxiSchema(Column[] Columns) {
    public static ParquetSharpTaxiSchema Full() {
        Column[] columns = [
            new Column<int?>("VendorID"),
            new Column<DateTime?>("tpep_pickup_datetime"),
            new Column<DateTime?>("tpep_dropoff_datetime"),
            new Column<long?>("passenger_count"),
            new Column<double?>("trip_distance"),
            new Column<long?>("RatecodeID"),
            new Column<string?>("store_and_fwd_flag"),
            new Column<int?>("PULocationID"),
            new Column<int?>("DOLocationID"),
            new Column<long?>("payment_type"),
            new Column<double?>("fare_amount"),
            new Column<double?>("extra"),
            new Column<double?>("mta_tax"),
            new Column<double?>("tip_amount"),
            new Column<double?>("tolls_amount"),
            new Column<double?>("improvement_surcharge"),
            new Column<double?>("total_amount"),
            new Column<double?>("congestion_surcharge"),
            new Column<double?>("Airport_fee")
        ];

        return new ParquetSharpTaxiSchema(columns);
    }

    public static ParquetSharpTaxiSchema Small() {
        Column[] columns = [
            new Column<int?>("VendorID"),
            new Column<long?>("passenger_count"),
            new Column<double?>("trip_distance"),
            new Column<long?>("RatecodeID"),
            new Column<long?>("payment_type"),
            new Column<double?>("fare_amount")
        ];
        return new ParquetSharpTaxiSchema(columns);
    }

    public WriterProperties CreateParquetSharpWriterProperties(LogicalEncoding encoding) {
        WriterPropertiesBuilder builder = new WriterPropertiesBuilder()
            .Compression(Compression.Snappy);

        switch(encoding) {
            case LogicalEncoding.RleDictionary:
                builder.EnableDictionary();
                builder.Encoding(Encoding.Plain);
                break;
            case LogicalEncoding.DeltaBinaryPacked:
                builder.DisableDictionary();
                builder.Encoding(Encoding.Plain);
                foreach(Column column in Columns) {
                    Type type = Nullable.GetUnderlyingType(column.LogicalSystemType)
                                   ?? column.LogicalSystemType;

                    if(type == typeof(int) || type == typeof(long)) {
                        builder.Encoding(column.Name, Encoding.DeltaBinaryPacked);
                    }
                }

                break;
            case LogicalEncoding.Plain:
                builder.DisableDictionary();
                builder.Encoding(Encoding.Plain);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(encoding), encoding, "Unknown logical encoding");
        }
        return builder.Build();
    }
}
