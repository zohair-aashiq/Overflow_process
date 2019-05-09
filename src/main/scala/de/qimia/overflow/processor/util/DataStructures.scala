package de.qimia.overflow.processor.util

import scala.collection.mutable

case class PostTags(uniqueID: Option[Long],
                    tags: String,
                    domain: Option[String])
    extends Serializable {
  val tagSet: Set[String] = tags.trim match {
    case "" => Set[String]()
    case _  => tags.trim.split(",").toSet
  }
  val tagHashNumSet: Set[Long] = tagSet.map(Util.hash)
  val uniqueTagCount: Int = tagHashNumSet.size
}

case class PostText(uniqueID: Option[Long],
                    title: mutable.ArrayBuffer[String],
                    body: mutable.ArrayBuffer[String],
                    domain: Option[String])
    extends Serializable

//case class tokenText(uniqueID:Option[Long], domain:Option[String],tokenText:Array[String])

case class tokensDictionary(uniqueID: Long, title: mutable.ArrayBuffer[String])

case class TextFeature(uniqueID: Long,
                       titleFeatures: org.apache.spark.ml.linalg.Vector,
                       bodyFeatures: org.apache.spark.ml.linalg.Vector)

case class TextsFeature(uniqueID: Long,
                        textFeatures: org.apache.spark.ml.linalg.Vector)

case class VectorizedCount(uniqueID: Long,
                           title: Array[Long],
                           body: Array[Long],
                           domain: String,
                           globalLabels: Set[Int],
                           originalTagCount: Int,
                           domainSpecificLabels: Set[Int],
                           set: String,
                           titleFeatures: org.apache.spark.ml.linalg.Vector,
                           bodyFeatures: org.apache.spark.ml.linalg.Vector)

case class ProcessedOccurrences(uniqueID: Long,
                                domain: String,
                                globalLabels: Set[Int],
                                originalTagCount: Int,
                                domainSpecificLabels: Set[Int],
                                titleFeatures: Map[Long, Int],
                                bodyFeatures: Map[Long, Int])

case class Tag(id: Int, label: String)

case class TagRanking(tagHash: Long,
                      tagName: String,
                      tagOccurrence: Long,
                      rank: Long)

case class ProcessedSample(uniqueID: Long,
                           title: Array[Long],
                           body: Array[Long],
                           domain: String,
                           globalLabels: Set[Int],
                           domainSpecificLabels: Set[Int],
                           originalTagCount: Int,
                           set: String)
    extends Serializable {
  def toCSVSample = {
    CSVProcessedSample(uniqueID,
                       title,
                       body,
                       domain,
                       globalLabels.mkString(""),
                       domainSpecificLabels.mkString(""),
                       originalTagCount,
                       set)
  }
}

case class ProcessedSampleText(uniqueID: Long,
                               title: Array[Long],
                               body: Array[Long],
                               domain: String,
                               globalLabels: Set[Int],
                               domainSpecificLabels: Set[Int],
                               originalTagCount: Int,
                               textFeatures: org.apache.spark.ml.linalg.Vector,
                               set: String)
    extends Serializable {
  def toCSVSample = {
    CSVProcessedSample(uniqueID,
                       title,
                       body,
                       domain,
                       globalLabels.mkString(""),
                       domainSpecificLabels.mkString(""),
                       originalTagCount,
                       set)
  }
}

case class CSVProcessedSample(uniqueID: Long,
                              title: Array[Long],
                              body: Array[Long],
                              domain: String,
                              globalLabels: String,
                              domainSpecificLabels: String,
                              originalTagCount: Int,
                              set: String)

case class TextKnowledge(token: String, hash: Long, rowID: Long)

case class SetSeparation(uniqueID: Long, dataset: Int)

case class IndexedToken(token: String)

case class DomainTag(tag: String,
                     domain: String,
                     tagInDomainCount: Long,
                     tagRankInDomain: Int)

case class GlobalTag(tag: String, tagGlobalRank: Int)

case class ProcessedTag(uniqueID: Long,
                        globalTags: Set[Int],
                        domainTags: Set[Int],
                        originalTagCount: Long,
                        domain: String)
    extends Serializable

case class ProcessedText(uniqueID: Long,
                         body: Array[Long],
                         title: Array[Long],
                         domain: String,
                         dataset: Int)
    extends Serializable

object DataStructures extends Serializable {
  type TextStorage = Array[String]
}

case class DictionaryItem(hash: Long, token: String, index: Int)
    extends Serializable
