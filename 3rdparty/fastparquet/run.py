""" fastparquet performance tests """

import time
from fastparquet import ParquetFile

def get_elapsed_time(start_time) -> str:
    """ Gets nicely formatted timespan from start_time to now """
    end = time.time()
    hours, rem = divmod(end-start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds)

if __name__ == "__main__":
   ROOT_PATH = "C:\\dev\\parquet-dotnet\\src\\Parquet.Test\\data\\"
   test_file = ROOT_PATH + "customer.impala.parquet"
   REPEAT = 10

   seconds_list = []

   print("reading {} {} times...".format(test_file, REPEAT))
   for i in range(0, REPEAT):
      start_time = time.time()
      pf = ParquetFile(test_file)
      df = pf.to_pandas()
      end_time = time.time()
      seconds = end_time - start_time
      seconds_list.append(seconds)
      print("file read in {}".format(get_elapsed_time(start_time)))

   print(seconds_list[1:])
   print(sum(seconds_list) / (len(seconds_list) - 1))