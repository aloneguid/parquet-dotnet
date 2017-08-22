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

val pqnet = spark.read.parquet("c:\\tmp\\fuckathena.parquet")
write(pqnet, "c:\\tmp\\fuckathenaspark.parquet")

pqnet.show


