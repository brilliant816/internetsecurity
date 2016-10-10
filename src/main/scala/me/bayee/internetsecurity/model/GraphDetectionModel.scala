package me.bayee.internetsecurity.model

import me.bayee.internetsecurity.pojo.HttpTrafficLog
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.XML

/**
  * Created by mofan on 16-9-18.
  */
object GraphDetectionModel extends App {

  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("graph_detection_model.xml"))
    val conf = new SparkConf().setAppName("GraphDetectionModel").setMaster("local[8]")
    val sc = new SparkContext(conf)
    // read the source
    val input = sc
      .textFile("/home/mofan/Documents/guoxiong/sanxilog/2016-01-27-30.log")
      .map(HttpTrafficLog.fromLine)

    // start the model
    val inDegreeZero = input.filter(_.referInfo.isEmpty)
    val filterUrl = inDegreeZero
      .map(htl => (htl.uri.getOrElse(""), htl.clientIp.getOrElse("")))
      .distinct
      .map(x => (x._1, 1))
      .reduceByKey(_ + _)
      .filter(_._2 <= 10)

    val base = inDegreeZero
      .map(htl => (htl.uri.getOrElse(""), htl))
      .leftOuterJoin(filterUrl)
      .filter(_._2._2.isDefined)
      .map(_._2._1)

    (xml \ "rules" \ "rule").foreach { node =>
      base.filter(htl => (node \ "regex").text.r.findFirstIn(htl.uri.getOrElse("")).isDefined)
        .map(_.toKeyValueWithScore((node \ "score").text))
        .saveAsSequenceFile((node \ "hdfsPath").text)
    }
  }
}