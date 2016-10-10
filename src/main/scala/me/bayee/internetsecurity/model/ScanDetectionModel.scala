package me.bayee.internetsecurity.model

import me.bayee.internetsecurity.pojo.HttpTrafficLog
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.XML

/**
  * Created by mofan on 16-10-1.
  */
object ScanDetectionModel extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("scan_detection_model.xml"))
    val conf = new SparkConf().setAppName("ScanDetectionModel").setMaster("local[8]")
    val sc = new SparkContext(conf)

    val input = sc
      .textFile("/home/mofan/Documents/guoxiong/sanxilog/2016-01-27-30.log")
      .map(HttpTrafficLog.fromLine)

    // useragent feature
    val blackList: List[String] = (xml \ "userAgent" \ "blackList" \ "value").map(_.text).toList
    input
      .filter(htl => blackList.foldLeft(false) { (last, value) => last match {
        case true => true
        case false => htl.userAgent.getOrElse("").contains(value)
      }
      })
      .map(_.toKeyValueWithScore((xml \ "userAgent" \ "score").text))
      .saveAsSequenceFile((xml \ "userAgent" \ "hdfsPath").text)
  }
}
