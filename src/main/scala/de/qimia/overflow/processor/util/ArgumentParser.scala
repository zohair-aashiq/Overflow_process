package de.qimia.overflow.processor.util

import java.io.File

import com.typesafe.config.ConfigFactory
import scopt.OParser

/**
  * An instance of this class will be passed around as program arguments.
  */
case class Config(
    inputPath: String = null,
    outputPath: String = null,
    trainingProportion: Double = 0.6,
    testProportion: Double = 0.3,
    validationProportion: Double = 0.1,
    fractionOfTags: Double = 1.0,
    seed: Long = 0,
    outputFormat: String = "parquet",
    rowCount: Int = 100,
    tokenUsageThreshold: Int = 5,
    vocabSize: Int = 100,
    nGrams: Int = 0,
    minDF: Int = 10,
    algorithmType: String = "idfModel",
    titleBodyCombination: Boolean = true,
    normalizer: Boolean = false,
    lowerCase: Boolean = false,
    stem: Boolean = false,
    host: String = "local[*]"
)

object ArgumentParser extends Serializable {

  def parse(args: Array[String]): Config = {
    if (args.isEmpty) {
      val parsed = parseConfigFile(ConfigFactory.load())
      Logger.append("Configs are read from a file on local server.")
      parsed
    } else {
      val parsed = parseArguments(args)
      Logger.append("Configs are taken from the command line interface.")
      parsed
    }
  }

  def parseConfigFile(parsed: com.typesafe.config.Config): Config = {
    val config = Config(
      inputPath = parsed.getString("inputPath"),
      outputPath = parsed.getString("outputPath"),
      trainingProportion = parsed.getDouble("trainingProportion"),
      testProportion = parsed.getDouble("testProportion"),
      validationProportion = parsed.getDouble("validationProportion"),
      fractionOfTags = parsed.getDouble("fractionOfTags"),
      seed = parsed.getLong("seed"),
      host = parsed.getString("host"),
      outputFormat = parsed.getString("outputFormat"),
      rowCount = parsed.getInt("rowCount"),
      tokenUsageThreshold = parsed.getInt("tokenUsageThreshold"),
      vocabSize = parsed.getInt("vocabSize"),
      minDF = parsed.getInt("minDF"),
      nGrams = parsed.getInt("nGrams"),
      algorithmType = parsed.getString("algorithmType"),
      titleBodyCombination = parsed.getBoolean("titleBodyCombination"),
      normalizer = parsed.getBoolean("normalizer"),
      lowerCase = parsed.getBoolean("lowerCase"),
      stem = parsed.getBoolean("stem")
    )
    config
  }

  def parseArguments(args: Array[String]): Config = {
    val builder = OParser.builder[Config]
    import builder._

    val parser1 = {
      OParser.sequence(
        programName("StackExchangeCSV-Interpreter"),
        head("StackExchangeCSV-Interpreterr", "0.1"),
        opt[String]('i', "input")
          .required()
          .validate(x =>
            if (new File(x).exists()) success
            else failure("The input directory does not exist"))
          .action((x, c) => c.copy(inputPath = x))
          .text("The input directory containing the CSV files."),
        opt[String]('o', "out")
          .required()
          .action((x, c) => {
            val fl = new File(x)
            if (!fl.exists) {
              fl.mkdirs()
              println("The output directory was created.")
            }
            c.copy(outputPath = x)
          })
          .text(
            "The output directory to save the Parquet data, label-key pairs and logs."),
        opt[Double]('f', "fraction-of-tags")
          .optional()
          .action((x, c) => c.copy(fractionOfTags = x))
          .text(
            "The fraction of the tags that are relevant. E.g. 0.41 discards 59% of the tags that are used less"),
        opt[Double]('t', "training-proportion")
          .optional()
          .action((x, c) => c.copy(trainingProportion = x))
          .text("The fraction of training set to the whole"),
        opt[Double]('e', "test-proportion")
          .optional()
          .action((x, c) => c.copy(testProportion = x))
          .text("The fraction of test set to the whole"),
        opt[Double]('v', "validation-proportion")
          .optional()
          .action((x, c) => c.copy(validationProportion = x))
          .text("The fraction of validation set to the whole"),
        opt[Long]('s', "seed")
          .optional()
          .action((x, c) => c.copy(seed = x))
          .text("The seed for the pseudo-random generator"),
        opt[Int]('r', "numRows")
          .optional()
          .action((x, c) => c.copy(rowCount = x))
          .text("The number of rows per file"),
        opt[Int]('u', "tokenUsageThreshold")
          .optional()
          .action((x, c) => c.copy(tokenUsageThreshold = x))
          .text("The number of rows per file"),
        opt[Int]('n', "N-Grams")
          .optional()
          .action((x, c) => c.copy(nGrams = x))
          .text("The N-Grams to extract. Default is 0, which means no N-Grams are extracted."),
        opt[String]("outputFormat")
          .optional()
          .action((x, c) => c.copy(outputFormat = x))
          .text("The format of the output. 'Parquet' or 'csv'."),
        opt[Int]("vocabSize")
          .optional()
          .action((x, c) => c.copy(vocabSize = x))
          .text("The number of vocabulary Size"),
        opt[Int]("minDF")
          .optional()
          .action((x, c) => c.copy(minDF = x))
          .text("The number of minDF"),
        opt[Boolean]("combine")
          .optional()
          .action((x, c) => c.copy(titleBodyCombination = x))
          .text("separate or combine"),
        opt[Boolean]("normalizer")
          .optional()
          .action((x, c) => c.copy(normalizer = x))
          .text("Using normalizer or not"),
        opt[Boolean]("lowerCase")
          .optional()
          .action((x, c) => c.copy(lowerCase = x))
          .text("lower-case or upper-case"),
        opt[Boolean]("stem")
          .optional()
          .action((x, c) => c.copy(stem = x))
          .text("Using stemmer or not"),
        help("help").text("help")
      )
    }

    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, Config()) match {
      case Some(config) => config
      case _ =>
        throw new IllegalArgumentException(
          "There is a problem with the parsed arguments.")
    }
  }
}
