import org.apache.spark.sql.{DataFrame, SparkSession}

val spark = SparkSession.builder()
  .appName("nested-records")
  .master("local[2]")
  .getOrCreate()

import spark.implicits._
val sc = spark.sparkContext

def write(df: DataFrame, path: String): Unit = {
  df
    .repartition(1)
    .write
    .mode("overwrite")
    .option("compression", "none")
    .parquet(path)
}

val df = spark.read.json("C:\\dev\\parquet-dotnet\\src\\Parquet.Test\\data\\nested-struct.json")

write(df, "C:\\dev\\parquet-dotnet\\src\\Parquet.Test\\data\\nested-struct.parquet")

df.show()


