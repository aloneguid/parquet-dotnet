// for performance tests only

using Parquet;
using Parquet.Data;
using Parquet.Schema;

// allocate stream large enough to avoid re-allocations during performance test
const int l = 10000000;
var ms = new MemoryStream(l * sizeof(int) * 2);
var schema = new ParquetSchema(new DataField<int>("id"));
var rnd = new Random();
int[] ints = new int[l];
for(int i = 0; i < l; i++) {
    ints[i] = rnd.Next();
}

using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
    using(ParquetRowGroupWriter g = writer.CreateRowGroup()) {
        await g.WriteColumnAsync(new DataColumn((DataField)schema[0], ints));
    }
}