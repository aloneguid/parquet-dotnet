import pyarrow as pa
#conda install pyarrow

data = [
   pa.array([1, 2, 3, 4]),
   pa.array(['foo', 'bar', 'baz', None]),
   pa.array([True, None, False, True])
]

batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])

print(batch)
print(batch.num_rows)
print(batch.num_columns)

sink = pa.BufferOutputStream()
writer = pa.RecordBatchFileWriter(sink, batch.schema)

for i in range(5):
   writer.write_batch(batch)

writer.close()

buf = sink.get_result()

with open('threecols.dat', 'wb') as f:
   f.write(buf)