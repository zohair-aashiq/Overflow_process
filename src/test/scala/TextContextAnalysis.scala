import java.nio.file.Files

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Murmur3Hash
import org.scalatest.FlatSpec
import de.qimia.overflow.processor.util.{Config, DataReader, Util}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg._
import de.qimia.overflow.processor.Process

import scala.collection.mutable

class TextContextAnalysis extends FlatSpec {
  val rootResources = getClass.getResource("/").getPath
  val astronomyData = rootResources + "/astronomy_data/"

  val hosts = List("local[*]")
  hosts.foreach(host => {
    val config = Config(
      inputPath = astronomyData,
      outputPath =
        Files.createTempDirectory("overflow-processor-astronomy").toString,
      host = host
    )
    val spark =
      Util.getSparkSession(config.host, "overflow-processor-test-local")

    import spark.implicits._

    val dataReader = DataReader.getInstance(spark, config.inputPath)

    val textData = dataReader.readPosts
    val tagData = dataReader.readPostTags

    Process.process(config)

    val dictionary = spark.read.parquet(s"${config.outputPath}/dictionary")
    val domainLabels =
      spark.read.parquet(s"${config.outputPath}/tags/domain_tags/")
    val globalLabels = spark.read.parquet(s"${config.outputPath}/global_tags")
    val bagWordsDict =
      spark.read.parquet(s"${config.outputPath}/bagWordsDictionary")
    val result = spark.read.parquet(s"${config.outputPath}/dataset")
    val stopRemover = spark.read.parquet(s"${config.outputPath}/stopRemover")
    stopRemover.where($"uniqueID" === 48).show(false)

    dictionary.show()
    bagWordsDict.show()
    //result.show()

    val titleWords = result
      .select("uniqueID", "title")
      .withColumn("titleWords", explode($"title"))
      .drop("title")
      .join(dictionary, dictionary.col("hash") === $"titleWords")
      .select("uniqueID", "token")
      .groupBy("uniqueID")
      .agg(concat_ws(",", collect_list("token")))
      .toDF("uniqueID", "titleWords") //.show()

    val bodyWords = result
      .select("uniqueID", "body")
      .withColumn("bodyWords", explode($"body"))
      .drop("body")
      .join(dictionary, dictionary.col("hash") === $"bodyWords")
      .select("uniqueID", "token")
      .groupBy("uniqueID")
      .agg(concat_ws(",", collect_list("token")))
      .toDF("uniqueID", "bodyWords") //.show()
    //titleWords.filter(titleWords.col("uniqueID") === 3).show(false)
    val sample = result

    val arrayDict = dictionary
      .select("hash", "token")
      .rdd
      .map(m => (m.getAs[Long](0), m.getAs[String](1)))
      .collectAsMap()
    val recoveredTitle = result
      .select("uniqueID", "title")
      .map(
        m =>
          (m.getLong(0),
           m.getAs[mutable.WrappedArray[Long]](1)
             .map(l => { arrayDict.getOrElse(l, "NOTFOUND") })))
      .toDF("uniqueID", "titleRecovered")

    val recoveredBody = result
      .select("uniqueID", "body")
      .map(
        m =>
          (m.getLong(0),
           m.getAs[mutable.WrappedArray[Long]](1)
             .map(l => { arrayDict.getOrElse(l, "NOTFOUND") })))
      .toDF("uniqueID", "bodyRecovered")

    recoveredBody.show
    recoveredTitle.show(false)
    val comparison =
      recoveredTitle.join(textData.select("uniqueID", "title"), Seq("uniqueID"))
    comparison.join(recoveredBody, Seq("uniqueID")).show()

    assert(comparison.count() == result.count())

    val bagWordsDictionary = bagWordsDict.rdd
      .map(m => (m.getAs[Int](1), m.getAs[String](0)))
      .collectAsMap()
    val recoveredBagWords = result
      .select("uniqueID", "titleBag")
      .map(
        m =>
          (m.getLong(0),
           m.getAs[SparseVector](1)
             .indices
             .map(l => { bagWordsDictionary.apply(l) })))
      .toDF()
      .show(false)

    result.filter($"uniqueID" === 327).select("uniqueID", "titleBag").show()

  })
}
