package de.qimia.overflow.processor.util

import java.io.{FileWriter, OutputStreamWriter}

import de.qimia.overflow.processor.util.Config
import org.apache.hadoop.fs.FSDataOutputStream

object Logger extends Serializable {
  var bufferBeforeWriting: List[String] = Nil
  var fw: OutputStreamWriter = _
  var hdfsFile: FSDataOutputStream = _

  def init(config: Config) = {
    config.host match {
//      case "yarn"=>
//        hdfsFile = Util.createHDFSFile(s"${config.outputPath}/log.txt")
//        fw = new OutputStreamWriter(hdfsFile, "UTF-8")
//        assert(fw != null)
//        fw.write("Log file created")
      case _ =>
        fw = new FileWriter(s"log.txt")
        assert(fw != null)
    }

  }

  def append(text: String): Unit = {
    println(text)
    if (fw == null)
      bufferBeforeWriting = text :: bufferBeforeWriting
    else {
      if (bufferBeforeWriting != Nil) {
        bufferBeforeWriting.foreach(x => fw.write(s"$x\n"))
        bufferBeforeWriting = Nil
      }
//      println(text)
      assert(fw != null)
      fw.write(s"$text\n")
      fw.flush()
    }
  }
  def close(): Unit = fw.close()
}
