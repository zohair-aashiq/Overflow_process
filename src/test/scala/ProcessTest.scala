import java.nio.file.Files

import org.scalatest.{BeforeAndAfter, FunSuite, _}
import Matchers._
import de.qimia.overflow.processor.util._
import org.apache.spark.ml.linalg.SparseVector

import de.qimia.overflow.processor.Process
import util._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
case class TokenizedText(uniqueID: Long,
                         title: Array[String],
                         body: Array[String],
                         domain: String)

class ProcessTest extends FlatSpec {

  def verifyRanking(data: List[TagRanking]): Boolean = data match {
    case head :: Nil => true
    case head :: next :: tail if head.rank + 1 == next.rank =>
      verifyRanking(next :: tail)
    case head :: next :: tail if head.rank + 1 != next.rank =>
      throw new IllegalArgumentException
  }

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

    //val tokenizedText = TextTokenizer.tokenize(textData,)

    s"File reader" must s"give correct checksum for host  $host" in {
      textData count () shouldBe 800
      tagData count () shouldBe 800
      textData.select("uniqueID").distinct().count() shouldBe 800
    }

    Process.process(config)

    println()
    val dictionary = spark.read.parquet(s"${config.outputPath}/dictionary")
    val domainLabels =
      spark.read.parquet(s"${config.outputPath}/tags/domain_tags/")
    val globalLabels = spark.read.parquet(s"${config.outputPath}/global_tags")
    val trainingSet =
      spark.read.parquet(s"${config.outputPath}/dataset/*/set=Training")
    val testSet = spark.read.parquet(s"${config.outputPath}/dataset/*/set=Test")
    val validationSet =
      spark.read.parquet(s"${config.outputPath}/dataset/*/set=Validation")
    val result = spark.read.parquet(s"${config.outputPath}/dataset")
    result.show
    val stopRemover = spark.read.parquet(s"${config.outputPath}/debug/cleanText")

//    //trainingSet.show()
//    result.sample(0.01).show()//.filter(result.col("uniqueID") === 113).show()//.sample(0.01).show()
//    //result.filter(result.col("uniqueID") === 393).show()//.sample(0.01).show()
//    stopRemover.sample(0.01).show()//
//    stopRemover.filter(stopRemover.col("uniqueID") === 3).show(false)
//    result.filter(result.col("uniqueID") === 3).select("titleFeatures").show(false)

    //
    if (config.lowerCase && config.normalizer) {
      if (!config.stem) {
        s"stopRemover" must s"remove words that don’t carry as much meaning" in {

          assert(
            stopRemover
              .filter(stopRemover.col("uniqueID") === 3)
              .select("title")
              .first()
              .get(0) === Array("sunspots", "appear", "dark"))
          assert(
            stopRemover
              .filter(stopRemover.col("uniqueID") === 9)
              .select("title")
              .first()
              .get(0) !== Array("How", "are", "black", "holes", "found"))

        }

        s"lower-case in normalizer" must s"work correctly" in {

          assert(
            stopRemover
              .filter(stopRemover.col("uniqueID") === 3)
              .select("title")
              .first()
              .get(0) !== Array("sunspots", "APPEAR", "dark"))

        }
      }
      if (config.stem) {
        s"Stemmer" must s"work correctly" in {
          assert(
            stopRemover
              .filter(stopRemover.col("uniqueID") === 3)
              .select("title")
              .first()
              .get(0) === Array("sunspot", "appear", "dark"))

        }

      }
    }

    s"join in joinTextsAndTags & bagWords" must s"give correct number of rows" in {

      result.select("uniqueID").distinct().count() shouldBe 800
    }

    if (!config.titleBodyCombination) {

      s"length of features" must s"be smaller than or equal to length of tokens" in {
        assert(
          result
            .filter(result.col("uniqueID") === 3)
            .select("title")
            .rdd
            .map(_.getAs[mutable.WrappedArray[Long]](0).length)
            .first() >=
            result
              .filter(result.col("uniqueID") === 3)
              .select("titleBag")
              .rdd
              .map(_.getAs[SparseVector](0).indices.length)
              .first())

        assert(
          result
            .filter(result.col("uniqueID") === 300)
            .select("title")
            .rdd
            .map(_.getAs[mutable.WrappedArray[Long]](0).length)
            .first() >=
            result
              .filter(result.col("uniqueID") === 300)
              .select("titleBag")
              .rdd
              .map(_.getAs[SparseVector](0).indices.length)
              .first())

        assert(
          result
            .filter(result.col("uniqueID") === 30)
            .select("title")
            .rdd
            .map(_.getAs[mutable.WrappedArray[Long]](0).length)
            .first() >=
            result
              .filter(result.col("uniqueID") === 30)
              .select("titleBag")
              .rdd
              .map(_.getAs[SparseVector](0).indices.length)
              .first())

        assert(
          result
            .filter(result.col("uniqueID") === 3)
            .select("body")
            .rdd
            .map(_.getAs[mutable.WrappedArray[Long]](0).length)
            .first() >=
            result
              .filter(result.col("uniqueID") === 3)
              .select("bodyBag")
              .rdd
              .map(_.getAs[SparseVector](0).indices.length)
              .first())

        assert(
          result
            .filter(result.col("uniqueID") === 300)
            .select("body")
            .rdd
            .map(_.getAs[mutable.WrappedArray[Long]](0).length)
            .first() >=
            result
              .filter(result.col("uniqueID") === 300)
              .select("bodyBag")
              .rdd
              .map(_.getAs[SparseVector](0).indices.length)
              .first())

        assert(
          result
            .filter(result.col("uniqueID") === 30)
            .select("body")
            .rdd
            .map(_.getAs[mutable.WrappedArray[Long]](0).length)
            .first() >=
            result
              .filter(result.col("uniqueID") === 30)
              .select("bodyBag")
              .rdd
              .map(_.getAs[SparseVector](0).indices.length)
              .first())

      }
    }

    //    val dictionaryMap = dictionary.collect.map{case (h, t, i) => h -> (t, i)}.toMap

    val totalSetSize: Long = trainingSet.count() + testSet
      .count() + validationSet.count()

    s"The program" must s"output same number of rows as input using host $host" in {
      val trainingCount = trainingSet.count().toInt
      val testCount = testSet.count().toInt
      val validationCount = validationSet.count().toInt

      trainingSet.count().toInt should be > 0
      testSet.count().toInt should be > 0
      validationSet.count().toInt should be > 0

      /**
        * The following four tests are probabilistic
        */
      assert(
        trainingCount.toFloat / validationCount.toFloat < config.trainingProportion / config.validationProportion * 1.2)
      assert(
        trainingCount.toFloat / validationCount.toFloat > config.trainingProportion / config.validationProportion * 0.8)

      assert(
        trainingCount.toFloat / testCount.toFloat < config.trainingProportion / config.testProportion * 1.2)
      assert(
        trainingCount.toFloat / testCount.toFloat > config.trainingProportion / config.testProportion * 0.8)

      trainingSet.count().toInt should be > testSet.count().toInt
      testSet.count().toInt should be > validationSet.count().toInt

      totalSetSize shouldBe 800
    }

    s"There" must "be samples which have more than one tag" in {
      trainingSet.printSchema()
      //trainingSet.show()
      trainingSet
        .select("domainSpecificLabels")
        .as[scala.collection.mutable.ArrayBuffer[Long]]
        .filter(x => x.size > 1)
        .count()
        .toInt should be > 0
      validationSet
        .select("domainSpecificLabels")
        .as[scala.collection.mutable.ArrayBuffer[Long]]
        .filter(x => x.size > 1)
        .count()
        .toInt should be > 0
      testSet
        .select("domainSpecificLabels")
        .as[scala.collection.mutable.ArrayBuffer[Long]]
        .filter(x => x.size > 1)
        .count()
        .toInt should be > 0
    }

    val subTrainingSet = trainingSet.sample(0.01)

    s"Dictionary" must "be learned correctly" in {
      val mockSet = spark.createDataset(
        Seq(
          (PostText(Some(1),
                    ArrayBuffer("A", "B", "C"),
                    ArrayBuffer("D"),
                    Some("")),
           SetSeparation(1, 0)),
          (PostText(Some(1),
                    ArrayBuffer("A", "B", "C"),
                    ArrayBuffer("D"),
                    Some("")),
           SetSeparation(1, 0)),
          (PostText(Some(1),
                    ArrayBuffer("A", "B", "C"),
                    ArrayBuffer("D"),
                    Some("")),
           SetSeparation(1, 0)),
          (PostText(Some(1),
                    ArrayBuffer("A", "B", "C"),
                    ArrayBuffer("D"),
                    Some("")),
           SetSeparation(1, 0)),
          (PostText(Some(1),
                    ArrayBuffer("A", "B", "C"),
                    ArrayBuffer("D"),
                    Some("")),
           SetSeparation(1, 0)),
          (PostText(Some(1),
                    ArrayBuffer("A", "B", "C"),
                    ArrayBuffer("D"),
                    Some("")),
           SetSeparation(1, 0)),
          (PostText(Some(2),
                    ArrayBuffer("D", "Y"),
                    ArrayBuffer[String](),
                    Some("")),
           SetSeparation(2, 0)),
          (PostText(Some(3), ArrayBuffer[String](), ArrayBuffer("X"), Some("")),
           SetSeparation(3, 0)),
          (PostText(Some(4),
                    ArrayBuffer[String](),
                    ArrayBuffer[String](),
                    Some("")),
           SetSeparation(4, 0)),
          (PostText(Some(4),
                    ArrayBuffer[String](),
                    ArrayBuffer[String](),
                    Some("")),
           SetSeparation(4, 0)),
          (PostText(Some(4),
                    ArrayBuffer[String](),
                    ArrayBuffer[String](),
                    Some("")),
           SetSeparation(4, 0)),
          (PostText(Some(4),
                    ArrayBuffer[String](),
                    ArrayBuffer[String](),
                    Some("")),
           SetSeparation(4, 0)),
          (PostText(Some(4),
                    ArrayBuffer[String](),
                    ArrayBuffer[String](),
                    Some("")),
           SetSeparation(4, 0)),
          (PostText(Some(4),
                    ArrayBuffer[String](),
                    ArrayBuffer[String](),
                    Some("")),
           SetSeparation(4, 0))
        ))
      val dictionary = Process
        .learnDictionary(spark, mockSet, config.copy(tokenUsageThreshold = 1))
        .collect()

      val expectedDict = Set("A", "B", "C", "D")
      dictionary.length shouldBe expectedDict.size

      dictionary.forall(expectedDict contains _.token) shouldBe true
    }

    s"Tags" must "be ranked correctly" in {
      val scalaMockSet = Seq(
        (PostTags(Some(0), "A", Some("2")), SetSeparation(1, 0)),
        (PostTags(Some(2), "A,B,E", Some("2")), SetSeparation(1, 0)),
        (PostTags(Some(3), "A,B,C", Some("2")), SetSeparation(1, 0)),
        (PostTags(Some(4), "A,B,E", Some("2")), SetSeparation(1, 0)),
        (PostTags(Some(5), "C,D", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(6), "C,F", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(7), "C,A,D", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(8), "C,F", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(9), "C,F", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(10), "C,E", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(11), "C,E", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(12), "C,E", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(13), "C,E", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(14), "C,E", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(15), "C,F", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(16), "C,E", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(17), "C,E", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(18), "C,E", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(19), "C,E", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(20), "C,E", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(21), "C,E", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(22), "C,E", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(23), "C,F", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(24), "C,A", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(25), "C,A", Some("1")), SetSeparation(1, 0)),
        (PostTags(Some(26), "", Some("1")), SetSeparation(1, 0))
      )
      val mockSet = spark.createDataset(scalaMockSet)
      val (ranking) = Process.rankTagsGlobally(spark, mockSet)
      //ranking.show

      val collectedRank =
        ranking.collect().map(x => x.tag -> x.tagGlobalRank).toMap
      val expectedDict = Set(
        GlobalTag("C", 0),
        GlobalTag("E", 1),
        GlobalTag("A", 2),
        GlobalTag("F", 3),
        GlobalTag("B", 4),
        GlobalTag("D", 5)
      )

      expectedDict.size shouldBe collectedRank.size

      expectedDict.foreach(x => collectedRank(x.tag) shouldBe x.tagGlobalRank)

      Process.rankTagsForDomain(spark, mockSet) //.show
      val domainRanking =
        Process.rankTagsForDomain(spark, mockSet).collect().toSet

      val expectedDomainRanking = Set(
        DomainTag("A", "2", 4, 0),
        DomainTag("B", "2", 3, 1),
        DomainTag("E", "2", 2, 2),
        DomainTag("C", "2", 1, 3),
        DomainTag("C", "1", 21, 0),
        DomainTag("E", "1", 12, 1),
        DomainTag("F", "1", 5, 2),
        DomainTag("A", "1", 3, 3),
        DomainTag("D", "1", 2, 4)
      )

      domainRanking.diff(expectedDomainRanking).size shouldBe 0
      expectedDomainRanking.diff(domainRanking).size shouldBe 0

      val extendedScalaMockTest = scalaMockSet ++ Seq(
        (PostTags(Some(256), "", Some("2")), SetSeparation(1, 0)),
        (PostTags(Some(123), "X", Some("2")), SetSeparation(1, 0))
      )

      val a = spark.createDataset(extendedScalaMockTest).map(_._1)

      val b = Process.mapTagRankings(spark,
                                     a,
                                     ranking,
                                     Process.rankTagsForDomain(spark, mockSet))

      //b.show

      val domainDict =
        domainRanking.map(x => (x.domain, x.tagRankInDomain) -> x.tag).toMap
      val globalTagDict = collectedRank.map(_.swap)

      /**
        * Check if domain and global tags have the same content.
        */
      b.collect().foreach(x => x.globalTags.size shouldBe x.domainTags.size)
      val convertedBack = b.map(x =>
        (x.uniqueID, x.globalTags.map(globalTagDict(_)), x.domainTags.map(y => {
          domainDict((x.domain, y))
        })))
      //convertedBack.show
      convertedBack.collect().foreach(x => x._2 shouldBe x._3)
    }

  })

}
