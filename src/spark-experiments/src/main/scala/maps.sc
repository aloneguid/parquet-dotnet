import org.apache.spark.sql.{DataFrame, SparkSession}

val spark = SparkSession.builder()
  .appName("maps")
  .master("local[2]")
  .getOrCreate()
val sc = spark.sparkContext
import spark.implicits._

def write(df: DataFrame, path: String): Unit = {
   df
      .repartition(1)
      .write
      .mode("overwrite")
      .option("compression", "none")
      .parquet(path)
}

val map = Map(1 -> "one", 2 -> "two", 3 -> "three")

val df = sc.parallelize(Seq(
   (1, map)
)).toDF("id", "numbers")

df.printSchema
df.show

write(df, "c:\\tmp\\sparkmap.parquet")