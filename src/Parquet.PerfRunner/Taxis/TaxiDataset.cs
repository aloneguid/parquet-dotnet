namespace Parquet.PerfRunner.Taxis;

record struct TaxiDataset(
    int?[] VendorID,
    DateTime?[] tpep_pickup_datetime,
    DateTime?[] tpep_dropoff_datetime,
    long?[] passenger_count,
    double?[] trip_distance,
    long?[] RatecodeID,
    string?[] store_and_fwd_flag,
    int?[] PULocationID,
    int?[] DOLocationID,
    long?[] payment_type,
    double?[] fare_amount,
    double?[] extra,
    double?[] mta_tax,
    double?[] tip_amount,
    double?[] tolls_amount,
    double?[] improvement_surcharge,
    double?[] total_amount,
    double?[] congestion_surcharge,
    double?[] Airport_fee
    ) {
}
