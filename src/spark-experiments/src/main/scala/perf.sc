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

val path = root + "postcodes.parquet"
val outPath = root + "write.test.parquet"

val ds = spark.read.csv(path)

var msl = List[Double]()

for(i <- 0 until 4) {
   val start = System.nanoTime
   val ds = spark.read.parquet(path)

   val localCopy = ds.collectAsList()

   //write(ds, outPath)

   val length = msFrom(start)
   msl = length :: msl
}

msl



