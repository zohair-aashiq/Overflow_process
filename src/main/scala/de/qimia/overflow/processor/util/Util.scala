package de.qimia.overflow.processor.util

import java.io.{File, FileNotFoundException}
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.hash.Hashing
import de.qimia.overflow.processor.util.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CommonConfigurationKeys, CommonConfigurationKeysPublic, FSDataOutputStream, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Util extends Serializable {

  /**
    * Given a host, returns a spark session on it; disables the warning logs on console.
    * @param host
    * @return
    */
  def getSparkSession(host: String, appName: String): SparkSession = {
    val session = SparkSession
      .builder()
      .appName(appName)
      .master(host)
      .getOrCreate()

    Logger.append(s"Spark session=$host, app=$appName")
    session.sparkContext.setLogLevel("WARN")
    session
  }

  def deleteFileIfExists(file: File): Unit = {
    if (file.exists()) file.delete()
  }
  def deleteFileIfExists(filePath: String): Unit = {
    deleteFileIfExists(new File(filePath))
  }

  val tokenHashFunction = Hashing.murmur3_128(0)

  def hash(token: String) = tokenHashFunction.hashString(token).asLong()

  def getHDFSServer = s"hdfs://m5848.contaboserver.net:8020/"

  def getHDFS() = {
    import org.apache.hadoop.fs.FileSystem
    val conf = new Configuration()
    conf.set("fs.defaultFS", getHDFSServer)
    FileSystem.get(conf)
  }

  def createHDFSFile(path: String): FSDataOutputStream = {
    val fs = getHDFS()
    assert(fs != null)
    val stream = fs.create(new Path(path))
    assert(stream != null)
    val ex = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      def run() = { stream.hflush(); stream.hsync(); stream.hflush() }
    }
    ex.scheduleAtFixedRate(task, 5, 3, TimeUnit.SECONDS)
    stream
  }

  def readHDFSFileToString(filePath: String): String = {
    val fs = getHDFS()
    val pth = new Path(filePath)
    if (!fs.exists(pth)) {
      throw new FileNotFoundException("The file is not found on hdfs.")
    }

    val stream = fs.open(pth)

    def readLines = scala.io.Source.fromInputStream(stream)

    readLines.mkString("")
  }

}
