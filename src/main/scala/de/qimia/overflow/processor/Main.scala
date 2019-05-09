package de.qimia.overflow.processor

import java.io.{File, FileOutputStream, FileReader}
import java.nio.file.Path
import java.util.Calendar

import de.qimia.overflow.processor.util._
import de.qimia.overflow.processor.util.Logger._

object Main extends Serializable {
  def main(args: Array[String]): Unit = {
    println("test")
    val config = ArgumentParser.parse(args)

    val start = System.currentTimeMillis()
    Logger.init(config)
    try {

      append(s"Started at ${Calendar.getInstance().getTime}")
      append(s"Input path: ${config.inputPath}")
      append(s"Output path: ${config.outputPath}")
      append(s"Seed: ${config.seed}")
      append(s"Training proportion: ${config.trainingProportion}")
      append(s"Validation proportion: ${config.validationProportion}")
      append(s"Test proportion: ${config.testProportion}")
      append(s"Output format: ${config.outputFormat}")
      append(s"Vocabulary Size: ${config.vocabSize}")
      append(s"Minimum DF: ${config.minDF}")
      append(s"N-Grams: ${config.nGrams}")
      append(s"Type of Algorithm for Features: ${config.algorithmType}")
      append(s"Token usage threshold: ${config.tokenUsageThreshold}")
      append(s"Combine body and title for bagOfWords: ${config.titleBodyCombination}")
      append(s"normalizer: ${config.normalizer}")
      append(s"lowerCase: ${config.lowerCase}")
      append(s"stemmer: ${config.stem}")

      val start = System.currentTimeMillis()

//
//      Console.withOut(config.host match {
//        //        case "yarn" => util.Util.createHDFSFile(s"${config.outputPath}/console.txt")
//        case _ => new FileOutputStream(s"console.txt")
//      }
//      ) {
      Process.process(config)
//      }

      append("")
      //append(s"Successfuly finished at ${Calendar.getInstance(un).getTime}")
    } catch {
      case e: Throwable =>
        append(s"FAILURE TO COMPLETE. ")
        append("Reason:")
        append(e.toString)
        append(e.getStackTrace.map(_.toString).mkString("\n"))
        e.printStackTrace()

    } finally {
      append(s"Total ${(System.currentTimeMillis() - start) / 1000} seconds.")
      append("\n\n\n\n\n")
      append("------------------------------------------------------------")
      append("<Input directory logs>")
      try {
        val inputLogContent =
          util.Util.readHDFSFileToString(s"${config.inputPath}/log.txt")
        append(inputLogContent)
      } catch {
        case _: Throwable =>
          append(
            scala.io.Source.fromFile(s"${config.inputPath}/log.txt").mkString)
      }
      append("</Input directory logs>")
      append("------------------------------------------------------------")

      close()
    }
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
}
