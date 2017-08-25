import org.apache.spark.sql.{DataFrame, SparkSession}

val spark = SparkSession.builder()
   .appName("nested-records")
   .master("local[2]")
   .getOrCreate()

import spark.implicits._
val sc = spark.sparkContext

val ds = spark.read.parquet("c:\\tmp\\decimals.parquet")
ds.printSchema()
ds.show()



