import org.apache.spark.sql.{DataFrame, SparkSession}

val spark = SparkSession.builder()
   .appName("performance")
   .master("local[1]")
   .getOrCreate()
val sc = spark.sparkContext

val root = "C:\\tmp\\"

def write(df: DataFrame, path: String): Unit = {
   df
      .repartition(1)
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(path)
}

def msFrom(start: Long) : Double = {
   val diff = System.nanoTime - start
   val ms = diff/1e6
   ms
}

val path = root + "postcodes.csv"

val ds = spark.read.csv(path)
ds.show()

/*var msl = List[Double]()

for(i <- 0 until 10) {
   val start = System.nanoTime
   val pqds = spark.read.parquet(path)

   write(pqds, outPath)

   val length = msFrom(start)
   msl = length :: msl
}

msl*/



