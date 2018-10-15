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

   def writeGzip(df: DataFrame, tag: String): Unit = {
      df
         .repartition(1)
         .write
         .mode("overwrite")
         .option("compression", "gzip")
         .parquet("c:\\tmp\\" + tag + ".parquet.folder")
   }

   val testFile = spark.read.parquet("c:\\tmp\\test.parquet")
   testFile.printSchema
   testFile.show(10, false)

   // PERF

   var i = 0
   var readTimes = List[Long]()
   var writeTimes = List[Long]()
   var writeGzTimes = List[Long]()
   for(i <- 1 to 10) {
      var from = System.currentTimeMillis
      val df = spark.read.parquet("C:\\dev\\parquet-dotnet\\src\\Parquet.Test\\data\\customer.impala.parquet").toDF()
      var to = System.currentTimeMillis
      readTimes = (to - from) :: Nil

      //uncompressed
      from = System.currentTimeMillis
      write(df, "unc")
      to = System.currentTimeMillis
      writeTimes = (to - from) :: Nil

      //gzip
      /*from = System.currentTimeMillis
      writeGzip(df, "gzip")
      to = System.currentTimeMillis
      writeGzTimes = (to - from) :: Nil*/
   }

   println("mean(read): " + readTimes.sum / readTimes.length)
   println("mean(write): " + writeTimes.sum / writeTimes.length)
   println("mean(write gzip): " + writeGzTimes.sum / writeGzTimes.length)

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
