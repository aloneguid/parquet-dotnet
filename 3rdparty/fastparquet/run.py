""" fastparquet performance tests """

import time
from fastparquet import ParquetFile
from fastparquet import write

def get_elapsed_time(start_time) -> str:
    """ Gets nicely formatted timespan from start_time to now """
    end = time.time()
    hours, rem = divmod(end-start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds)

def avg(name, times):
   val = sum(times[1:]) / (len(times) - 1)
   print("average for {}: {}".format(name, val))


if __name__ == "__main__":
   ROOT_PATH = "C:\\dev\\parquet-dotnet\\src\\Parquet.Test\\data\\"
   test_file = ROOT_PATH + "customer.impala.parquet"
   write_file = "c:\\tmp\\write.test.parquet"
   REPEAT = 4

   read_times = []
   write_gzip_times = []
   write_snappy_times = []
   write_uncompressed_times = []

   print("reading and writing {} {} times...".format(test_file, REPEAT))
   for i in range(0, REPEAT):
      start_time = time.time()
      pf = ParquetFile(test_file)
      df = pf.to_pandas()
      read_times.append(time.time() - start_time)
      print("file read in {}".format(get_elapsed_time(start_time)))

      start_time = time.time()
      write(write_file, df, compression="UNCOMPRESSED")
      write_uncompressed_times.append(time.time() - start_time)
      print("written uncompressed")

      start_time = time.time()
      write(write_file, df, compression="GZIP")
      write_gzip_times.append(time.time() - start_time)
      print("written gzip")

      #start_time = time.time()
      #write(write_file, df, compression="SNAPPY")
      #write_snappy_times.append(time.time() - start_time)
      #print("written snappy")



   avg("read", read_times)
   avg("uncompressed write", write_uncompressed_times)
   avg("gzip write", write_gzip_times)
   #avg("snappy write", write_snappy_times)
