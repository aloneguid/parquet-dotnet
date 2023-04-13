# reference - https://arrow.apache.org/docs/python/parquet.html

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# pa.Table.from

# parquet_file = pq.ParquetFile("1.parquet")

df = pd.DataFrame({
    "ints": [1, 2, 3],
    "tags": [[1, 2], [3, 4], [5, 6]]
}, index=list("abc"))

table = pa.Table.from_pandas(df, preserve_index=False)

pq.write_table(table, "1.parquet", compression="none")