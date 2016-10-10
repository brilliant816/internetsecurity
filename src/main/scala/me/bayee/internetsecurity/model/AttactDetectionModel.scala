package me.bayee.internetsecurity.model

import me.bayee.internetsecurity.pojo.HttpTrafficLog
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.XML

/**
  * Created by mofan on 16-9-27.
  */
object AttactDetectionModel extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("attact_detection_model.xml"))
    val conf = new SparkConf().setAppName("AttactDetectionModel").setMaster("local[8]")
    val sc = new SparkContext(conf)

    val input = sc
      .textFile("/home/mofan/Documents/guoxiong/sanxilog/2016-01-27-30.log")
      .map(HttpTrafficLog.fromLine)

    // start the model
    val base = input.filter(_.httpCode.getOrElse(-1) == 200)

    (xml \ "rules" \ "rule").foreach { node =>
      base.filter(htl => (node \ "regex").text.r.findFirstIn(htl.uri.getOrElse("")).isDefined)
        .map(_.toKeyValueWithScore((node \ "score").text))
        .saveAsSequenceFile((node \ "hdfsPath").text)
    }
  }
}
