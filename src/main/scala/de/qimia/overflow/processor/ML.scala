package de.qimia.overflow.processor

import de.qimia.overflow.processor.util._
import org.apache.spark.ml.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object ML extends App {

  val total_tags = 100

  val spark = Util.getSparkSession("local[*]", "Overflow-Data-Analysis")

  import spark.implicits._

  val config = ArgumentParser.parse(Array[String]())
  val t = spark.read
    .parquet(s"${config.outputPath}/dataset")
    .where($"domain" === "math.stackexchange.com")
  println(s"size ${t.count()}")
//  t.show()
    val dataset = t
    .select("uniqueID", "title", "body", "domainSpecificLabels", "set")
      .as[(Long, ArrayBuffer[Long], ArrayBuffer[Long], ArrayBuffer[Long], String)]
      .map{case (id, a, b, c, str)=>(id, a, b, c.filter(_<total_tags), str)}
      .toDF("uniqueID", "title", "body", "domainSpecificLabels", "set")


  val processed= dataset
    .as[(Long, ArrayBuffer[Long], ArrayBuffer[Long], ArrayBuffer[Long], String)]
    .map{case (id, t, b, l, s)=>(id, (t.map(-1*_.abs)++b.map(-1*_.abs)++l).toSet, s)}
    .toDF("id", "itemss", "set")
//  dataset.show(false)

  val training = processed.where($"set"==="Training")
  val validation = processed.where($"set"==="Validation")
  val test = processed.where($"set"==="Test")
//  println("Total training samples")
//  println(training.count())
  val fitted = new FPGrowth()
    .setPredictionCol("pred")
    .setItemsCol("itemss")
    .setMinConfidence(0.7)
    .setMinSupport(20.0/training.count())
    .fit(training)

//  fitted.freqItemsets.show()
//  fitted.associationRules.show()
//  fitted.associationRules.printSchema()
//  fitted.associationRules.withColumn("consequent", explode($"consequent")).where($"consequent">0).show(false)

//  new AssociationRules().

  def testOnSet(data:DataFrame, rules:FPGrowthModel)={

    val processed= data
      .as[(Long, ArrayBuffer[Long], ArrayBuffer[Long], ArrayBuffer[Long], String)]
      .map{case (id, t, b, l, s)=>(id, (t.map(-1*_.abs)++b.map(-1*_.abs)).toSet, l, s)}
      .toDF("id", "itemss", "correct_labels","set")


    val transformed = rules.transform(processed)
    val classification = transformed.as[(Long, ArrayBuffer[Long], ArrayBuffer[Long], String, ArrayBuffer[Long])]
        .map{case (id, items, cl, set, pred) => (id, items, cl.toSet, set, pred.filter(_>=0).toSet)}
        .map{case (id, items, cl, set, pred) => (id, pred intersect cl, pred diff cl, cl diff pred)}
    classification.toDF("id", "TP", "FP", "FN").show()
    val res = classification.map{case (id, tp, fp, fn)=> (id, tp.size, fp.size, fn.size)}
      .reduce{(l, r) => (l, r) match {case ((_,lTP,lFP,lFN),(_,rTP,rFP,rFN))=>(0,lTP+rTP,lFP+rFP,lFN+rFN)}}
    println(s"TP: ${res._2}, FP: ${res._3}, FN: ${res._4}")
    val prec = (res._2.toFloat)/((res._2+res._3).toFloat)
    val rec = (res._2.toFloat)/((res._2+res._4).toFloat)
    val f1 = prec*rec*2.0/(prec+rec)
    println(s"Prec: $prec, Rec: $rec, F1: $f1")

    transformed.show()
  }

  println("Training")
  testOnSet(dataset.where($"set"==="Training"), fitted)
  println("Validation")
  testOnSet(dataset.where($"set"==="Validation"), fitted)
  println("Test")
  testOnSet(dataset.where($"set"==="Test"), fitted)

  fitted.transform(validation)
  val tags = spark.read
    .parquet(s"${config.outputPath}/global_tags")
//
//  dataset.show()
//  tags.orderBy("tagGlobalRank").show
//
//  val dict = spark.read
//    .parquet(s"${config.outputPath}/dictionary")
//
//  val a = dataset
//    .withColumn("title", -abs(explode($"title")))
//    .join(dict, $"title" === $"hash", "left")
//    .na.drop()
////    .fill("NOTFOUND")
//    .groupBy("uniqueID")
//    .agg(
//      collect_list("token").as("title"),
//      first("body").as("body"),
//      first("domain").as("domain"),
//      first("set").as("set"),
//      first("globalLabels").as("globalLabels")
////      first("titleBodyBag").as("titleBodyBag")
//    )
//
//  a.show(false)
//  val b = a
//    .withColumn("body", -abs(explode($"body")))
//    .join(dict, $"body" === $"hash", "left")
//    .na
//    .fill("NOTFOUND")
//    .groupBy("uniqueID")
//    .agg(
//      collect_list("token").as("body"),
//      first("title").as("title"),
//      first("domain").as("domain"),
//      first("set").as("set"),
//      first("globalLabels").as("globalLabels")
////      first("titleBodyBag").as("titleBodyBag")
//    )
//
//  val c = b
//    .withColumn("globalLabels", explode($"globalLabels"))
//    .join(tags, $"globalLabels" === $"tagGlobalRank")
//    .groupBy("uniqueID")
//    .agg(
//      first("body").as("body"),
//      first("title").as("title"),
//      first("domain").as("domain"),
//      first("set").as("set"),
//      collect_list("tag").as("tag")
////      first("titleBodyBag").as("titleBodyBag")
//    )
//
//  println("BBBBB")
//  b.show()
//  println("CCCCC")
//
//  c.show(false)
//  dict.show()
//
//  val processed =
//    b.withColumn("input", array_union(array_union($"title", $"body"), $"globalLabels"))
//
//  println("FEATURES")
//  processed.show()
//  val training = processed.where($"set"==="Training")
//  val validation = processed.where($"set"==="Validation")
//  val test = processed.where($"set"==="Test")
//  val fitted = new FPGrowth()
//    .setPredictionCol("pred")
//    .setItemsCol("input")
//    .setMinConfidence(0.5)
//    .setMinSupport(0.01)
//    .fit(training)
//
//  fitted.freqItemsets.show()
//  fitted.associationRules.show()
//  fitted.associationRules.printSchema()
//
//  val rules = fitted.associationRules.withColumn("consequent", explode($"consequent"))
//  rules.show

//  println(fitted.itemSupport.mkString("\n"))
}
