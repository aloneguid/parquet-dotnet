import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}

val spark = SparkSession.builder()
   .appName("analyse-devices")
   .master("local[1]")
   .getOrCreate()

import spark.implicits._
val sc = spark.sparkContext

val devices = spark.read.parquet("c:\\tmp\\out\\device_10m.parquet")

val i = 0
