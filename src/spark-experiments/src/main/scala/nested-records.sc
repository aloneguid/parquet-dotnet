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

def convert(name: String, writeFile: Boolean): DataFrame = {
  val df = spark
     .read
     .option("wholeFile", true)
     .option("mode", "PERMISSIVE")
     .json(root + name + ".json")
  if(writeFile) {
    write(df, root + name + ".dir.parquet")
  }
  df
}

def convertFromFormattedJson(name: String, writeFile: Boolean): DataFrame = {
   val df = spark
      .read
      .json(sc.wholeTextFiles(root + name + "*.json").values)

   if(writeFile) {
      write(df, root + name + ".dir.parquet")
   }
   df
}

val df = convertFromFormattedJson("nested", true)

df.printSchema()
