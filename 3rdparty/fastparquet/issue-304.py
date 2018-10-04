from fastparquet import ParquetFile
from fastparquet import write
import pandas as pd
import numpy as np

s = pd.DataFrame({'test': np.random.randn(1_000_000)})
s[::2] = np.nan
s.to_parquet('test.parquet', compression='gzip')