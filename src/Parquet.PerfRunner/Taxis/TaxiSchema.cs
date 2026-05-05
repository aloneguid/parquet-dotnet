using Parquet.Schema;

namespace Parquet.PerfRunner.Taxis;

sealed class TaxiSchema {

    public ParquetSchema Schema { get; }
    public TaxiColumn[] Columns { get; }

    private TaxiSchema(ParquetSchema schema, TaxiColumn[] columns) {
        Schema = schema;
        Columns = columns;
    }

    public static TaxiSchema Full(TaxiDataset dataset) {
        ParquetSchema schema = new(
            new DataField<int?>("VendorID"),
            new DateTimeDataField("tpep_pickup_datetime", DateTimeFormat.Timestamp, isNullable: true, unit: DateTimeTimeUnit.Micros),
            new DateTimeDataField("tpep_dropoff_datetime", DateTimeFormat.Timestamp, isNullable: true, unit: DateTimeTimeUnit.Micros),
            new DataField<long?>("passenger_count"),
            new DataField<double?>("trip_distance"),
            new DataField<long?>("RatecodeID"),
            new DataField<string?>("store_and_fwd_flag"),
            new DataField<int?>("PULocationID"),
            new DataField<int?>("DOLocationID"),
            new DataField<long?>("payment_type"),
            new DataField<double?>("fare_amount"),
            new DataField<double?>("extra"),
            new DataField<double?>("mta_tax"),
            new DataField<double?>("tip_amount"),
            new DataField<double?>("tolls_amount"),
            new DataField<double?>("improvement_surcharge"),
            new DataField<double?>("total_amount"),
            new DataField<double?>("congestion_surcharge"),
            new DataField<double?>("Airport_fee")
        );
        DataField[] dataFields = schema.GetDataFields();
        TaxiColumn[] columns = [
            new(dataFields[0], rowGroup => rowGroup.WriteAsync<int>(dataFields[0], dataset.VendorID)),
            new(dataFields[1], rowGroup => rowGroup.WriteAsync<DateTime>(dataFields[1], dataset.tpep_pickup_datetime)),
            new(dataFields[2], rowGroup => rowGroup.WriteAsync<DateTime>(dataFields[2], dataset.tpep_dropoff_datetime)),
            new(dataFields[3], rowGroup => rowGroup.WriteAsync<long>(dataFields[3], dataset.passenger_count)),
            new(dataFields[4], rowGroup => rowGroup.WriteAsync<double>(dataFields[4], dataset.trip_distance)),
            new(dataFields[5], rowGroup => rowGroup.WriteAsync<long>(dataFields[5], dataset.RatecodeID)),
            new(dataFields[6], rowGroup => rowGroup.WriteAsync(dataFields[6], dataset.store_and_fwd_flag)),
            new(dataFields[7], rowGroup => rowGroup.WriteAsync<int>(dataFields[7], dataset.PULocationID)),
            new(dataFields[8], rowGroup => rowGroup.WriteAsync<int>(dataFields[8], dataset.DOLocationID)),
            new(dataFields[9], rowGroup => rowGroup.WriteAsync<long>(dataFields[9], dataset.payment_type)),
            new(dataFields[10], rowGroup => rowGroup.WriteAsync<double>(dataFields[10], dataset.fare_amount)),
            new(dataFields[11], rowGroup => rowGroup.WriteAsync<double>(dataFields[11], dataset.extra)),
            new(dataFields[12], rowGroup => rowGroup.WriteAsync<double>(dataFields[12], dataset.mta_tax)),
            new(dataFields[13], rowGroup => rowGroup.WriteAsync<double>(dataFields[13], dataset.tip_amount)),
            new(dataFields[14], rowGroup => rowGroup.WriteAsync<double>(dataFields[14], dataset.tolls_amount)),
            new(dataFields[15], rowGroup => rowGroup.WriteAsync<double>(dataFields[15], dataset.improvement_surcharge)),
            new(dataFields[16], rowGroup => rowGroup.WriteAsync<double>(dataFields[16], dataset.total_amount)),
            new(dataFields[17], rowGroup => rowGroup.WriteAsync<double>(dataFields[17], dataset.congestion_surcharge)),
            new(dataFields[18], rowGroup => rowGroup.WriteAsync<double>(dataFields[18], dataset.Airport_fee))
        ];

        return new TaxiSchema(schema, columns);
    }

    public static TaxiSchema Small(TaxiDataset dataset) {
        ParquetSchema schema = new(
            new DataField<int?>("VendorID"),
            new DataField<long?>("passenger_count"),
            new DataField<double?>("trip_distance"),
            new DataField<long?>("RatecodeID"),
            new DataField<long?>("payment_type"),
            new DataField<double?>("fare_amount")
        );
        DataField[] dataFields = schema.GetDataFields();
        TaxiColumn[] columns = [
            new(dataFields[0], rowGroup => rowGroup.WriteAsync<int>(dataFields[0], dataset.VendorID)),
            new(dataFields[1], rowGroup => rowGroup.WriteAsync<long>(dataFields[1], dataset.passenger_count)),
            new(dataFields[2], rowGroup => rowGroup.WriteAsync<double>(dataFields[2], dataset.trip_distance)),
            new(dataFields[3], rowGroup => rowGroup.WriteAsync<long>(dataFields[3], dataset.RatecodeID)),
            new(dataFields[4], rowGroup => rowGroup.WriteAsync<long>(dataFields[4], dataset.payment_type)),
            new(dataFields[5], rowGroup => rowGroup.WriteAsync<double>(dataFields[5], dataset.fare_amount))
        ];

        return new TaxiSchema(schema, columns);
    }
}

public sealed record TaxiColumn(DataField Field, Func<ParquetRowGroupWriter, Task> WriteAsync);
