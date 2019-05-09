package de.qimia.overflow.processor

import de.qimia.overflow.processor.util.{ArgumentParser, Util}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable.ArrayBuffer

object FPGrowthProcessor extends App {
  val spark = Util.getSparkSession("local[*]", "Overflow-Data-Analysis")

  import spark.implicits._

  val config = ArgumentParser.parse(Array[String]())
  val t = spark.read
    .parquet(s"${config.outputPath}/dataset")
    .where($"domain" === "math.stackexchange.com")

  val training = t.where($"set"==="Training")
  val validation = t.where($"set"==="Validation")
  val test = t.where($"set"==="Test")


  def process(dat:DataFrame, set:String)={
    val data = dat.select("title", "body", "domainSpecificLabels")
      .as[(ArrayBuffer[Long], ArrayBuffer[Long], ArrayBuffer[Long])]
      .map { case (t, b, l) => (t.map(-1L * _.abs).toSet union b.map(-1L * _.abs).toSet, l.toSet) }
    val transactions = data.map { case (tokens, l) => tokens union l }

    transactions
        .map(_.mkString(" "))
        .coalesce(1)
      .write
      .mode("overwrite")
      .option("delimiter", " ")
      .option("header", false)
      .option("quote", "")
      .mode("overwrite")
      .csv(s"${config.outputPath}/$set.transactions")

    transactions.flatMap(x=>x).distinct()
      .map(x=> x.toString + " " + (if(x<0)"body" else "head"))
      .toDF("both")
      .coalesce(1)
      .write
      .option("header", true)
      .option("quote", "")
      .mode("overwrite")
      .csv(s"${config.outputPath}/$set.appearances")

  }
  process(training, "training")
  process(validation, "validation")
  process(test, "test")
}




