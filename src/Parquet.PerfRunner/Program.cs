// for performance tests only

using Parquet;
using Parquet.Data;

// allocate stream large enough to avoid re-allocations during performance test
const int l = 1000000;
var ms = new MemoryStream(l * sizeof(int) * 2);
var schema = new Schema(new DataField<int>("id"));
var rnd = new Random();
var ints = new int[l];
for(int i = 0; i < l; i++) {
    ints[i] = rnd.Next();
}

using(var writer = await ParquetWriter.CreateAsync(schema, ms)) {
    using(var g = writer.CreateRowGroup()) {
        await g.WriteColumnAsync(new DataColumn((DataField)schema[0], ints));
    }
}