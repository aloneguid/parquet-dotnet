import sys
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

if __name__ == "__main__":

    tbl: pa.Table = pq.read_table(sys.argv[1])

    # tbl has .to_pydict() method, but it fails on complex datatypes
    # convert Arrow table to Pandas DataFrame, which can handle JSON conversion
    df: pd.DataFrame = tbl.to_pandas()

    j: str = df.to_json()
    print(j)
