package de.qimia.overflow.processor

import com.johnsnowlabs.nlp.annotator.SentenceDetector
import com.johnsnowlabs.nlp.annotators.{Normalizer, Stemmer, Tokenizer}
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher, RecursivePipeline}
import de.qimia.overflow.processor.util.Config
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.{NGram, StopWordsRemover}
import org.apache.spark.sql.DataFrame

object TextTokenizer extends Serializable {
  var output: DataFrame = _
  var stemmer: Stemmer = _
  var normalizer: Normalizer = _

  def tokenize(data: DataFrame, config: Config) = {

    val processed = List("body", "title")
      .foldRight(data)((t, b) => {
        var outCol: String = s"${t}Tokenized"
        var stages = List[PipelineStage]()

        val assembler = new DocumentAssembler()
          .setInputCol(s"${t}")
          .setOutputCol(s"${t}Document")
          .setTrimAndClearNewLines(true)

        stages = stages :+ assembler

        val sentenceDetector = new SentenceDetector()
          .setInputCols(s"${t}Document")
          .setOutputCol(s"${t}Sentence")

        stages = stages :+ sentenceDetector

        val tokenizer = new Tokenizer()
          .setInputCols(s"${t}Sentence")
          .setOutputCol(outCol)

        stages = stages :+ tokenizer

        if (config.normalizer) {
          normalizer = new Normalizer()
            .setInputCols(outCol)
            .setLowercase(config.lowerCase)
            .setOutputCol(s"${t}Normalized")
          outCol = s"${t}Normalized"
          stages = stages :+ normalizer
        }
        if (config.stem) {
          stemmer = new Stemmer()
            .setInputCols(outCol)
            .setOutputCol(s"${t}Stem")
          outCol = s"${t}Stem"
          stages = stages :+ stemmer
        }

        val finisher = new Finisher()
          .setInputCols(outCol)
          .setOutputCols(s"${t}Finished")
        stages = stages :+ finisher

        val stopRemover = new StopWordsRemover()
          .setInputCol(s"${t}Finished")
          .setOutputCol(s"${t}Clean")
          .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))

        stages = stages :+ stopRemover

        if (config.nGrams>1){
          val nGram = new NGram()
            .setN(config.nGrams)
            .setInputCol(s"${t}Clean")
            .setOutputCol(s"${t}NGrams")
          stages = stages :+ nGram
        }

        output = new RecursivePipeline()
          .setStages(stages.toArray)
          .fit(b)
          .transform(b)

        output

      })
//    processed.select("titleNGrams", "titleClean").show()
    var cols = Array[String]("uniqueID", "titleClean", "bodyClean", "domain")
    if (config.nGrams>1){
      cols = cols:+"titleNGrams"
      cols = cols:+"bodyNGrams"
    }
      processed.select(cols.head, cols.tail:_*)
      .withColumnRenamed("titleClean", "title")
      .withColumnRenamed("bodyClean", "body")
  }

}
