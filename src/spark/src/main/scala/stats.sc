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
case class Book(id: Int, isbn: String, author: String)

val ds = Seq(
   Book(3, "12345-6", "Ivan Gavryliuk"),
   Book(1, "12345-7", "GitHub Contributors"),
   Book(2, "12345-8", "More GitHub Contributors")
).toDS

ds.show
ds.printSchema()

// this will write stats for ID column (min, max) nicely
write(ds.toDF, "stat")
