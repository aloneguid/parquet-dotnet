import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, SparkSession}

val spark = SparkSession.builder()
   .appName("primitives")
   .master("local[2]")
   .getOrCreate()

import spark.implicits._
val sc = spark.sparkContext

spark.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")

val root = "C:\\dev\\parquet-dotnet\\src\\Parquet.Test\\data\\"

def write(df: DataFrame, path: String): Unit = {
   df
      .repartition(1)
      .write
      .mode("overwrite")
      .option("compression", "none")
      .parquet(path)
}

//decimals
val validDecimal: BigDecimal = 1.2
val nullDecimal: BigDecimal = null
val negDecimal: BigDecimal = -1
//negDecimal.setScale(2, BigDecimal.RoundingMode.FLOOR)
var dfDec = sc.parallelize(Seq(
   (1, validDecimal, nullDecimal, negDecimal)
))
   .toDF("id", "validDecimal", "nullDecimal", "negDecimal")

dfDec = dfDec.withColumn("negDecimal", dfDec.col("negDecimal").cast(DecimalType(10, 2)))

//repeatables
val dfRep2 = sc.parallelize(Seq(
   (1, Array[String]("1", "2", "3")),
   (2, Array[String]()),
   (3, Array[String]("1", "2", "3")),
   (4, Array[String]())
)).toDF("id", "repeats2")

val dfRep1 = sc.parallelize(Seq(
   (2, Array[String]())
)).toDF("id", "repeats1")

//maps in structs
case class StructSample(id: Int, map: Map[Int, String])

val map = Map(1 -> "one", 2 -> "two", 3 -> "three")
val dfMapsInStructs = sc.parallelize(Seq(
   (1, new StructSample(1, map))
)).toDF("id", "strcture")

val df = dfDec
df.printSchema
df.show
write(df, root + "decimallegacy.folder.parquet")



