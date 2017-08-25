import org.apache.spark.sql.{DataFrame, SparkSession}

val spark = SparkSession.builder()
   .appName("nested-records")
   .master("local[2]")
   .getOrCreate()

import spark.implicits._
val sc = spark.sparkContext

val root = "C:\\dev\\parquet-dotnet\\src\\Parquet.Test\\data\\"

def write(df: DataFrame, path: String): Unit = {
   df
      .repartition(1)
      .write
      .mode("overwrite")
      .option("compression", "none")
      .parquet(path)
}

//create new dataset with primitives

val decimalSample: BigDecimal = 1.2

val df = sc.parallelize(Seq(
   (1, decimalSample)
)).toDF("id", "decimal")

df.show
df.printSchema
df.schema.prettyJson

write(df, root + "complex-primitives.folder.parquet")


