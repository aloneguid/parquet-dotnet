using Parquet.Data;
using Parquet.Schema;

namespace Parquet.PerfRunner.Taxis;

sealed class TaxiSchema {

    public ParquetSchema Schema { get; }
    public DataColumn[] Columns { get; }

    private TaxiSchema(ParquetSchema schema, DataColumn[] columns) {
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
        DataField[] dataFields = schema.GetDataFields(); // better API not available in older version we compare against.
        DataColumn[] columns = [
            new DataColumn(dataFields[0], dataset.VendorID),
            new DataColumn(dataFields[1], dataset.tpep_pickup_datetime),
            new DataColumn(dataFields[2], dataset.tpep_dropoff_datetime),
            new DataColumn(dataFields[3], dataset.passenger_count),
            new DataColumn(dataFields[4], dataset.trip_distance),
            new DataColumn(dataFields[5], dataset.RatecodeID),
            new DataColumn(dataFields[6], dataset.store_and_fwd_flag),
            new DataColumn(dataFields[7], dataset.PULocationID),
            new DataColumn(dataFields[8], dataset.DOLocationID),
            new DataColumn(dataFields[9], dataset.payment_type),
            new DataColumn(dataFields[10], dataset.fare_amount),
            new DataColumn(dataFields[11], dataset.extra),
            new DataColumn(dataFields[12], dataset.mta_tax),
            new DataColumn(dataFields[13], dataset.tip_amount),
            new DataColumn(dataFields[14], dataset.tolls_amount),
            new DataColumn(dataFields[15], dataset.improvement_surcharge),
            new DataColumn(dataFields[16], dataset.total_amount),
            new DataColumn(dataFields[17], dataset.congestion_surcharge),
            new DataColumn(dataFields[18], dataset.Airport_fee)
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
        DataColumn[] columns = [
            new DataColumn(dataFields[0], dataset.VendorID),
            new DataColumn(dataFields[1], dataset.passenger_count),
            new DataColumn(dataFields[2], dataset.trip_distance),
            new DataColumn(dataFields[3], dataset.RatecodeID),
            new DataColumn(dataFields[4], dataset.payment_type),
            new DataColumn(dataFields[5], dataset.fare_amount)
        ];

        return new TaxiSchema(schema, columns);
    }
}
