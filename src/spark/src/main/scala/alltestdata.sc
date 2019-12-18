import org.apache.spark.sql.{DataFrame, SparkSession}

//http://spark.apache.org/docs/latest/sql-programming-guide.html#programmatically-specifying-the-schema

val spark = SparkSession.builder()
  .appName("alltestdata")
  .master("local[1]")
  .getOrCreate()
val sc = spark.sparkContext
import spark.implicits._

def write(df: DataFrame, tag: String): Unit = {
   df
      .repartition(1)
      .write
      .mode("overwrite")
      .option("compression", "none")
      .parquet("c:\\tmp\\" + tag + ".parquet.folder")
}

// MAPS
/*val mapSimple = Map(1 -> "one", 2 -> "two", 3 -> "three")
var df = sc.parallelize(Seq(
   (1, mapSimple)
)).toDF("id", "numbers")
write(df, "maps")
df = sc.parallelize(Seq(
   (1, mapRepeatableKey)
)).toDF(colNames = "id", "strings")
write(df, "mapsrk")*/

// STRUCT

case class Book(isbn: String, author: String)

val ds = Seq(
   Book("12345-6", "Ivan Gavryliuk"),
   Book("12345-7", "Richard Conway")
).toDS

ds.show
//ds.printSchema()
write(df, "struct_plain")
