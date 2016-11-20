package me.bayee.internetsecurity.model

import me.bayee.internetsecurity.pojo.{HttpTrafficLog, ModelInput}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._
import scala.xml.XML
import me.bayee.internetsecurity.util.StringUtil._

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
      .newAPIHadoopFile((xml \ "input").text, classOf[AvroKeyInputFormat[GenericRecord]], classOf[AvroKey[GenericRecord]], classOf[NullWritable])
      .map(_._1.datum.toString.parseJson.convertTo[HttpTrafficLog])
      .distinct

    (xml \ "rules" \ "rule").foreach { node =>
      val group = input
        .map(htl => ((node \ "regex").text.r.findFirstIn(htl.uri.getOrElse("").base64Decode.urlDecode.getParamString).getOrElse(""), htl))
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
          .flatMap(_._2.map(_.toModelInput.toKeyValueWithId((node \ "id").text)))
          .saveAsSequenceFile((node \ "hdfsPath").text)
      }
    }
  }
}
