package de.qimia.overflow.processor

import java.io.FileOutputStream

import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel
import org.apache.spark.ml.feature._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import de.qimia.overflow.processor.util.Logger._
import de.qimia.overflow.processor.util._


object Process extends Serializable {

  val TRAINING = 0
  val TEST = 1
  val VALIDATION = 2

  val SET_NAMES = Array("Training", "Test", "Validation")
  val UNDEFINED_DOMAIN = "undefined_domain"
  var cleaner: Dataset[PostText] = _
  var features: DataFrame = _

  def process(config: Config): Unit = {

    val spark = Util.getSparkSession(config.host, "Overflow-Processor")

    import spark.implicits._
    spark.sparkContext.hadoopConfiguration
      .setEnum("parquet.summary.metadata.level", JobSummaryLevel.ALL)

    val dataReader = DataReader.getInstance(spark, inputPath = config.inputPath)

    println()

    append(s"Input path: ${config.inputPath}")
    append("Reading and tokenizing the text")
    val tokenizerStart = System.currentTimeMillis()
    val ps = dataReader.readPosts

    /**
      * tokenizing and cleaning
      */
    val cleanText = processText(spark, ps, config)

    //    append(s"processed ${spark.sparkContext.broadcast(postTexts.count()).value} post texts")
    append(s"Took ${System.currentTimeMillis() - tokenizerStart} milliseconds.")

    val postTagLists = dataReader.readPostTags

    append("processed tags")

    /**
      * Separate the dataset
      */
    val proportions = spark.sparkContext.broadcast(
      Array(config.trainingProportion,
            config.testProportion,
            config.validationProportion))

    val dataSeparation = postTagLists
      .map(a => a.uniqueID)
      .withColumn("rnd", rand())
      .as[(Long, Double)]
      .withColumn(
        "rnd",
        when($"rnd" < proportions.value(0), TRAINING)
          .when($"rnd" < proportions.value(1) + proportions.value(0), TEST)
          .otherwise(VALIDATION))
      .toDF("uniqueID", "dataset")
      .as[SetSeparation]

    val tags = postTagLists
      .as("ptl")
      .joinWith(dataSeparation.as("ds"), $"ptl.uniqueID" === $"ds.uniqueID")
    val text = cleanText
      .as("pt")
      .joinWith(dataSeparation.as("ds"), $"pt.uniqueID" === $"ds.uniqueID")

    /**
      * Get the training set
      */
    val trainingTagSet = tags
      .filter((tuple: (PostTags, SetSeparation)) =>
        tuple._2.dataset == TRAINING)
      .cache()
    val trainingTextSet = text.filter((tuple: (PostText, SetSeparation)) =>
      tuple._2.dataset == TRAINING)

    /**
      * Learn tags from the training set
      */
    val domainSpecificTags = rankTagsForDomain(spark, trainingTagSet).cache()
    val numDomainSpecificTags = domainSpecificTags
      .groupBy("domain")
      .agg(count("tag"))
      .as[(String, Long)]
      .map(x => s"${x._1} -> ${x._2}")
      .collect()

    val globalTags = rankTagsGlobally(spark, trainingTagSet).cache()

    val dictionary = learnDictionary(spark, trainingTextSet, config).cache()

    /**
      * Process texts
      */
    val processedText = processTexts(spark, text).cache()
    val numRows = spark.sparkContext.broadcast(config.rowCount)

    val processedTags =
      mapTagRankings(spark, tags.map(_._1), globalTags, domainSpecificTags)

    /**
      * made option for combination of title and body for features.
      */
    val (baggedTokens, tokenBagDict) = bagWords(
      spark,
      cleanText.toDF(),
      config
    )
    val joined =
      joinTextsAndTags(spark, processedText, processedTags, baggedTokens)
        .cache()

    val dataWriter =
      joined
        .repartition($"domain", $"set")
        .withColumn("rng", round(monotonically_increasing_id / numRows.value))
        .write
        .mode("overwrite")

    val globalTagsWriter =
      globalTags.coalesce(1).write.mode("overwrite").option("delimiter", "|")
    val domainTagsWriter = domainSpecificTags
      .coalesce(1)
      .write
      .partitionBy("domain")
      .mode("overwrite")
      .option("delimiter", "|")
    val dictionaryWriter =
      dictionary.coalesce(1).write.mode("overwrite").option("delimiter", "|")

    writeMetaData(numDomainSpecificTags,
                  dictionary.toDF(),
                  globalTags.toDF(),
                  tokenBagDict,
                  numRows.value,
                  config)

    config.outputFormat.toLowerCase match {
      case "csv" =>
        //        data.map{_.toCSVSample}.coalesce(1).write.partitionBy("domain", "set", "rng")
        //          .mode("overwrite").option("delimiter", "|").csv(config.outputPath+"/dataset")

        globalTagsWriter.csv(config.outputPath + "/global_tags")
        domainTagsWriter.csv(config.outputPath + "/tags/domain_tags")
        dictionaryWriter.csv(s"${config.outputPath}/dictionary")
        tokenBagDict
          .coalesce(1)
          .write
          .mode("overwrite")
          .csv(s"${config.outputPath}/bagWordsDictionary")

      case "parquet" =>
        dataWriter
          .partitionBy("domain", "set", "rng")
          .mode("overwrite")
          .parquet(config.outputPath + "/dataset")
        globalTagsWriter.parquet(config.outputPath + "/global_tags")
        domainTagsWriter.parquet(config.outputPath + "/tags/domain_tags")
        dictionaryWriter.parquet(s"${config.outputPath}/dictionary")
        tokenBagDict
          .coalesce(1)
          .write
          .mode("overwrite")
          .parquet(s"${config.outputPath}/bagWordsDictionary")
        append("Done writing essential files")
    }
  }

  /**
    * Writes the given stats to a metadata file
    * @param numDomainTags
    * @param dictionary
    * @param globalTags
    * @param tokenBagDict
    * @param numRows
    * @param config
    */
  def writeMetaData(numDomainTags: Seq[String],
                    dictionary: DataFrame,
                    globalTags: DataFrame,
                    tokenBagDict: DataFrame,
                    numRows: Long,
                    config: Config):Unit = {

    val metaData = Map(
      "#Unique Tags Per Domain" -> ("[\n\t" + numDomainTags
        .mkString("\n\t") + "\n]"),
      "#Words in sequential Dict" -> dictionary.count(),
      "#UniqueTags Globally" -> globalTags.count(),
      "#Rows" -> numRows,
      "#Words in word bag Dict" -> tokenBagDict.count()
    )

    val metaDataFile = if (config.host == "yarn") {
      Util.createHDFSFile(s"${config.outputPath}/metadata")
    } else {
      new FileOutputStream(s"${config.outputPath}/metadata")
    }

    metaDataFile.write(metaData.map(x => x._1 + ": " + x._2).mkString("\n").getBytes)
    metaDataFile.flush()
    metaDataFile.close()
  }

  /**
    * From the training set, gather all distinct words and assign a unique hash value and a continuous index value for
    * each of them.
    *
    * @param sparkSession
    * @param text Since we do not want to leak information from the test/validation sets, this must be training set.
    * @return a dictionary dataset where a unique hash value and an index is generated for each token.
    */
  def learnDictionary(sparkSession: SparkSession,
                      text: Dataset[(PostText, SetSeparation)],
                      config: Config): Dataset[DictionaryItem] = {
    import sparkSession.implicits._
    val threshold =
      sparkSession.sparkContext.broadcast(config.tokenUsageThreshold)
    text
      .flatMap { case (p, _) => p.title ++ p.body }
      .toDF("token")
      .groupBy("token")
      .agg(count($"token").as("tokenCount"))
      .where($"tokenCount" > threshold.value)
      .select("token")
      .coalesce(1)
      .withColumn("index", monotonically_increasing_id)
      .as[(String, Long)]
      .map {
        case (str, index) => DictionaryItem(Util.hash(str), str, index.toInt)
      }
  }

  /**
    * Given the list of tags for each post, sort the tags with respect to the number of occurences across all domains.
    *
    * @param spark
    * @param trainingSet
    * @return A tuple where the first element is the key value pair of the tag string and its global rank.
    *         Second element is the number of unique tags in the training set.
    */
  def rankTagsGlobally(
      spark: SparkSession,
      trainingSet: Dataset[(PostTags, SetSeparation)]): Dataset[GlobalTag] = {

    import spark.implicits._
    numPartitions(trainingSet.toDF(), "beforeGRanking")
    val tags = trainingSet
      .withColumn("tag", explode(split($"_1.tags", ",")))
      .select("tag")
      .filter($"tag" =!= "")

    val fitted = new StringIndexer()
      .setInputCol("tag")
      .setOutputCol("tagGlobalRank")
      .fit(tags)
    val globalRanking = fitted
      .transform(tags.distinct())
      .as[(String, Double)]
      .withColumn("tagGlobalRank", $"tagGlobalRank".cast("integer"))

    val dSet = globalRanking.as[GlobalTag].cache()
    numPartitions(dSet.toDF(), "afterGRanking")

    dSet
  }

  def numPartitions(df: DataFrame, name: String): Unit = {
    println(s"Number of Partitions for $name: ${df.rdd.partitions.length}.")
  }

  /**
    * Similar to the @rankTagsGlobally, but the ranking is done domain-wise.
    *
    * @param sparkSession
    * @param trainingSet
    * @return A dataset of (domain-tag-tagCountInDomain-tagRankInDomain) tuples.
    */
  def rankTagsForDomain(
      sparkSession: SparkSession,
      trainingSet: Dataset[(PostTags, SetSeparation)]): Dataset[DomainTag] = {

    import sparkSession.implicits._
    val domainBasedCounts = trainingSet
      .flatMap {
        case (a, _) =>
          a.tagSet.map(x => (a.domain.getOrElse(UNDEFINED_DOMAIN), x))
      }
      .toDF("domain", "tag")
      .groupBy("domain", "tag")
      .agg(count("tag"))
      .toDF("domain", "tag", "tagInDomainCount")

    val domainBasedRanking = domainBasedCounts
      .withColumn(
        "tagRankInDomain",
        row_number().over(
          Window.partitionBy("domain").orderBy($"tagInDomainCount".desc)))
      .withColumn("tagRankInDomain", $"tagRankInDomain".cast("int") - 1)
      .repartition(200)

    domainBasedRanking
      .select("domain", "tag", "tagInDomainCount", "tagRankInDomain")
      .as[DomainTag]

  }

  /**
    * The body and title fields are tokenized
    *
    * @param sparkSession
    * @param data
    * @return
    */
  def processText(sparkSession: SparkSession,
                  data: DataFrame,
                  config: Config): Dataset[PostText] = {
    import sparkSession.implicits._
    val a = TextTokenizer.tokenize(data, config)
    a.as[PostText]
  }

  /**
    * dropped all the stop words from the input sequences
    *
    * @param sparkSession
    * @param col1
    * @param col2
    * @param data
    * @return
    */
  def removeStopWords(sparkSession: SparkSession,
                      data: Dataset[PostText]): Dataset[PostText] = {
    import sparkSession.implicits._

    val remover = new StopWordsRemover()
      .setInputCol("title")
      .setOutputCol("cleanTitle")
      .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))

    val cleanTitle = remover
      .transform(data)
      .drop("title")
      .withColumnRenamed("cleanTitle", "title")
    val cleanBody = remover
      .setInputCol("body")
      .setOutputCol("cleanBody")
      .transform(cleanTitle)
      .drop("body")
      .withColumnRenamed("cleanBody", "body")
    cleanBody.as[PostText]
  }

  /**
    * Tag strings are replaced by their ranks. If the tag is not found it is omitted. To indicate this, tag count before
    * the mapping is kept in originalTagCount
    *
    * @param tagSet
    */
  def mapTagRankings(sparkSession: SparkSession,
                     tagSet: Dataset[PostTags],
                     globalTags: Dataset[GlobalTag],
                     domainTags: Dataset[DomainTag]): Dataset[ProcessedTag] = {

    import sparkSession.implicits._

    /**
      * By using Options and left join, the posts which have tags that do not exist in training set are assigned an empty set.
      * If they have no tags at all, they won't exist in this dataset
      */
    val set1 = tagSet
      .flatMap(t =>
        t.tagSet.map(x => (t.uniqueID, t.uniqueTagCount, x, t.domain)))
      .toDF("uniqueID", "uniqueTagCount", "tag", "domain")
      .join(globalTags, Seq("tag"), "left")
      .join(domainTags, Seq("tag", "domain"), "left")

    set1
      .groupBy("uniqueID")
      .agg(
        collect_set("tagGlobalRank").as("globalTags"),
        collect_set("tagRankInDomain").as("domainTags"),
        first("uniqueTagCount").as("originalTagCount"),
        first("domain").as("domain")
      )
      .select("uniqueID",
              "globalTags",
              "domainTags",
              "originalTagCount",
              "domain")
      .as[ProcessedTag]
  }

  /**
    * Maps each token to a 64-bit hash value
    *
    * @param texts
    * @param textKnowledge
    */
  def processTexts(
      sparkSession: SparkSession,
      texts: Dataset[(PostText, SetSeparation)]): Dataset[ProcessedText] = {
    import sparkSession.implicits._
    texts.map {
      case (p, s) =>
        ProcessedText(p.uniqueID.get,
                      p.body.toArray.map(Util.hash),
                      p.title.toArray.map(Util.hash),
                      p.domain.get,
                      s.dataset)
    }
  }

  /**
    *
    * Create the dataset from the texts and tags with the same uniqueID.
    *
    * @param sparkSession
    * @param text
    * @param tags
    * @param textFeatures
    * @return
    */
  def joinTextsAndTags(sparkSession: SparkSession,
                       text: Dataset[ProcessedText],
                       tags: Dataset[ProcessedTag],
                       textFeatures: DataFrame) = {
    import sparkSession.implicits._

    /**
      * By use of Options the posts which have no tags are mapped to have empty set of tags.
      */
    text
      .as("tx")
      .joinWith(tags.as("tg"), $"tg.uniqueID" === $"tx.uniqueID", "left")
      .as[(ProcessedText, Option[ProcessedTag])]
      .map {
        case (txt, tgs) =>
          (txt, tgs.getOrElse(ProcessedTag(-1, Set[Int](), Set[Int](), 0, ""))) match {
            case (pText, pTag) =>
              ProcessedSample(pText.uniqueID,
                              pText.title,
                              pText.body,
                              pText.domain,
                              pTag.globalTags,
                              pTag.domainTags,
                              pTag.originalTagCount.toInt,
                              SET_NAMES(txt.dataset))
          }
      }
      .as("tx")
      .join(textFeatures.as("tf"), Seq("uniqueID"), "left")
  }

  /**
    *
    * Compute the occurrence of each token in a sequence from text.
    * Maps each token to a 64-bit hash value.
    *
    * @param sparkSession
    * @param data
    * @param data
    * @param tok
    * @return
    */
  def occurrencesInText(sparkSession: SparkSession,
                        data: Dataset[ProcessedSample],
                        tok: Dataset[PostText]) = {
    import sparkSession.implicits._
    data
      .as("dt")
      .joinWith(tok.as("tk"), $"dt.uniqueID" === $"tk.uniqueID")
      .map {
        case (dt, tk) =>
          ProcessedOccurrences(
            dt.uniqueID,
            dt.domain,
            dt.globalLabels,
            dt.originalTagCount,
            dt.domainSpecificLabels,
            tk.title.toArray
              .map(Util.hash)
              .groupBy(l => l)
              .map(t => (t._1, t._2.length)),
            tk.body.toArray
              .map(Util.hash)
              .groupBy(l => l)
              .map(t => (t._1, t._2.length))
          )
      }
  }

  /**
    *
    * Create the Feature-Vectors from title and body.
    *
    * @param sparkSession
    * @param processedTexts
    * @param config
    * @return
    */
  def bagWords(sparkSession: SparkSession,
               processedTexts: DataFrame,
               config: Config): Tuple2[DataFrame, DataFrame] = {
    import sparkSession.implicits._

    /**
      * If N-Grams are used, merge title and body with their respective N-Gram representations
      */
    val nGramsMerged = if (config.nGrams > 1) {
      processedTexts
        .withColumn("title", concat($"title", $"titleNGrams"))
        .withColumn("body", concat($"body", $"bodyNGrams"))
        .drop("titleNGrams", "bodyNGrams")
    } else { processedTexts }
    val titleBody = nGramsMerged
      .withColumn("titleBody", concat($"title", $"body")) //.sample(0.05)}

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("titleBody")
      .setOutputCol("_titleBody")
      .setVocabSize(config.vocabSize)
      .setMinDF(config.minDF)
      .fit(titleBody)

    val titleBodyBag = cvModel
      .transform(titleBody)
      .drop("titleBody")
      .withColumnRenamed("_titleBody", "titleBodyBag")
      .select("uniqueID", "titleBodyBag")
    val dict = cvModel.vocabulary.zipWithIndex.toList.toDF("words", "rank")

    val cvTransformed =
      if (!config.titleBodyCombination) {
        val bodyTransformed = cvModel
          .setInputCol("body")
          .setOutputCol("bodyBag")
          .transform(titleBody)
        cvModel
          .setInputCol("title")
          .setOutputCol("titleBag")
          .transform(bodyTransformed)
          .select("uniqueID", "titleBag", "bodyBag")
      } else {
        titleBodyBag
      }

    if (config.algorithmType == "idfModel") {
      val idfModel = new IDF()
        .setInputCol("titleBodyBag")
        .setOutputCol("_titleBodyBag")
        .fit(titleBodyBag)

      val idfData = if (config.titleBodyCombination) {
        val data = idfModel
          .transform(cvTransformed)
          .drop("titleBodyBag")
          .withColumnRenamed("_titleBodyBag", "titleBodyBag")
        data
      } else {
        val bod = idfModel
          .setInputCol("bodyBag")
          .setOutputCol("_bodyBag")
          .transform(cvTransformed)
          .drop("bodyBag")
          .withColumnRenamed("_bodyBag", "bodyBag")

        val titl = idfModel
          .setInputCol("titleBag")
          .setOutputCol("_titleBag")
          .transform(bod)
          .drop("titleBag")
          .withColumnRenamed("_titleBag", "titleBag")
        titl
      }
      (idfData, dict)
    } else {
      (cvTransformed, dict)
    }
  }

}
