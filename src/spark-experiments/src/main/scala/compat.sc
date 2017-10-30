import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}

val spark = SparkSession.builder()
   .appName("nested-records")
   .master("local[2]")
   .getOrCreate()

import spark.implicits._
val sc = spark.sparkContext

def readCsv(path: String): DataFrame = {
   spark.sqlContext.read
     .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
     .load(path)
}

def write(df: DataFrame, path: String): Unit = {
   df
      .repartition(1)
      .write
      .mode("overwrite")
      .option("compression", "uncompressed")
      .parquet(path)
}

val pqnet = readCsv("c:\\tmp\\postcodes.csv")
pqnet.printSchema()

val clean = pqnet
   .withColumnRenamed("In Use?", "InUse")
   .withColumnRenamed("Built up area", "BuiltUpArea")
   .withColumnRenamed("Built up sub-division", "BuiltUpSubDivision")
   .withColumnRenamed("Lower layer super output area", "LowerLayerSuperOutputArea")
.withColumnRenamed("Rural/urban", "RuralUrban")
.withColumnRenamed("London zone", "LondonZone")
.withColumnRenamed("LSOA Code", "LSOACode")
.withColumnRenamed("Local authority", "LocalAuthority")
.withColumnRenamed("MSOA Code", "MSOACode")
.withColumnRenamed("Middle layer super output area", "MiddleLayerSuperOutputArea")

print(clean.count())
//write(clean, "c:\\tmp\\postcodes.parquet.dir")

pqnet.show




