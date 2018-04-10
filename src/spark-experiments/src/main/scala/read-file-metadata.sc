import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsScalaMapConverter}

val spark = SparkSession.builder()
   .appName("random")
   .master("local[1]")
   .getOrCreate()

import spark.implicits._
val sc = spark.sparkContext

val df = spark.read.parquet("C:\\tmp\\metadata.parquet")
df.printSchema()
df.show()

val conf = spark.sparkContext.hadoopConfiguration

def getFooters(conf: Configuration, path: String) = {
   val fs = FileSystem.get(conf)
   val footers = ParquetFileReader.readAllFootersInParallel(conf, fs.getFileStatus(new Path(path)))
   footers
}

def getFileMetadata(conf: Configuration, path: String) = {
   getFooters(conf, path)
      .asScala.map(_.getParquetMetadata.getFileMetaData.getKeyValueMetaData.asScala)
}

val metadata = getFileMetadata(conf, "C:\\tmp\\metadata.parquet")

