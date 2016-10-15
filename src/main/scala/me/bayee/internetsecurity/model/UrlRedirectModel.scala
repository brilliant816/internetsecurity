package me.bayee.internetsecurity.model

import me.bayee.internetsecurity.pojo.ModelInput
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.XML

/**
  * Created by mofan on 16-10-16.
  */
object UrlRedirectModel extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("url_redirect_model.xml"))
    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val input = sc
      .textFile((xml \ "input").text)
      .map(ModelInput.fromHttpTrafficLog)

    (xml \ "rules" \ "rule").foreach { node =>
      val group = input
        .map(mi => ((node \ "regex").text.r.findFirstIn(mi.url.getOrElse("")).getOrElse(""), mi))
        .filter(_._1.nonEmpty)
        .groupByKey()

      val count = group.count()
      val mean = group.map(_._2.size).reduce(_ + _).toDouble / count.toDouble

      if (count < (node \ "count").text.toLong) {
        group
          .map { tuple =>
            val list = tuple._2.toList
            (list.size, list)
          }
          .filter(_._1 > mean * (node \ "mean").text.toDouble)
          .flatMap(_._2.map(_.toKeyValueWithId((node \ "id").text)))
          .saveAsSequenceFile((node \ "hdfsPath").text)
      }
    }
  }
}
