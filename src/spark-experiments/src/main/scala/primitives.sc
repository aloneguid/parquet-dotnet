import org.apache.spark.sql.{DataFrame, SparkSession}

val spark = SparkSession.builder()
   .appName("primitives")
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

val validDecimal: BigDecimal = 1.2
val nullDecimal: BigDecimal = null

val df = sc.parallelize(Seq(
   (1, validDecimal, nullDecimal)
)).toDF("id", "validDecimal", "nullDecimal")

df.show
df.printSchema
df.schema.prettyJson

write(df, root + "decimalnulls.folder.parquet")



