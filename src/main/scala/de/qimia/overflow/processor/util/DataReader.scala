package de.qimia.overflow.processor.util

import java.io

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

class DataReader(spark: SparkSession, inputPath: String) extends Serializable {

  import spark.implicits._

  def readPosts: DataFrame = {
    val posts = spark.read
      .option("header", false)
      .option("sep", "|")
      .option("multiLine", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .schema(
        new StructType()
          .add("uniqueID", "long")
          .add("title", "string")
          .add("body", "string")
          .add("domain", "string"))
      .csv(s"$inputPath/postText*.csv")
//      .withColumn("file_name", input_file_name())
//      .repartition($"file_name")
//      .drop("file_name")
      .na
      .fill("")
    posts.withColumn("domain", regexp_replace(posts.col("domain"), "\\r", ""))
    .withColumn("title", regexp_replace(posts.col("title"), "((\\$\\$.*?\\$\\$)|(\\$.*?\\$)|(\\\\begin.*?\\\\end\\{.*?}))", ""))
    .withColumn("body", regexp_replace(posts.col("body"), "((\\$\\$.*?\\$\\$)|(\\$.*?\\$)|(\\\\begin.*?\\\\end\\{.*?}))", ""))
  }

  def readPostTags: Dataset[PostTags] = {
    val pathToRead = s"$inputPath/postTag*.csv"
    Logger.append(s"Tags are read from $pathToRead")
    val posts = spark.read
      .option("header", false)
      .option("sep", "|")
      .option("multiLine", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .schema(
        new StructType()
          .add("uniqueID", "long")
          .add("tags", "string")
          .add("domain", "string"))
      .csv(pathToRead)
      .na
      .fill("")
    posts
      .withColumn("domain", regexp_replace(posts.col("domain"), "\\r", ""))
      .as[PostTags]

  }

  def distinctDomains: Array[String] = {
    val inputDir = new java.io.File(inputPath)
    inputDir.list.filter(x => new io.File(x).isDirectory)
  }

}

object DataReader {

  var instance: DataReader = null
  def getInstance(spark: SparkSession, inputPath: String): DataReader = {
    if (instance == null) {
      instance = new DataReader(spark, inputPath)
    }
    instance
  }
}
