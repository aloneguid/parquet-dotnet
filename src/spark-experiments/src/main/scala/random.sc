import org.apache.spark.sql.{DataFrame, SparkSession}

val spark = SparkSession.builder()
   .appName("random")
   .master("local[1]")
   .getOrCreate()

import spark.implicits._
val sc = spark.sparkContext

val df = spark.read.parquet("c:\\tmp\\com.parquet")
df.printSchema()
df.show()
