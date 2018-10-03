package com.ivan.parquet
import org.apache.spark.sql.{DataFrame, SparkSession}

object ScalaApp extends App {
   //http://spark.apache.org/docs/latest/sql-programming-guide.html#programmatically-specifying-the-schema

   System.setProperty("hadoop.home.dir", "C:\\Users\\ivang\\OneDrive\\Software\\hadoop-win\\hadoop-3.0.0")

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

   val testFile = spark.read.parquet("c:\\tmp\\test.parquet")
   testFile.printSchema
   testFile.show(10, false)

   //EMPTY LIST

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

   /*case class AuthorInfo(firstName: String, lastName: String)

   case class Book(isbn: String, author: AuthorInfo)

   val ds = Seq(
      Book("12345-6", AuthorInfo("Ivan", "Gavryliuk")),
      Book("12345-7", AuthorInfo("Richard", "Conway"))
   ).toDS

   ds.printSchema()
   ds.show
   //ds.printSchema()
   write(ds.toDF, "struct_plain")*/



}
