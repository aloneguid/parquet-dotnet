import sys
import pyarrow.parquet as pq
import json

if __name__ == "__main__":

    tbl = pq.read_table(sys.argv[1])

    pd = tbl.to_pydict()

    print(json.dumps(pd))