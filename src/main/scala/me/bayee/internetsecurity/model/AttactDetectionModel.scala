package me.bayee.internetsecurity.model

import java.net.URLDecoder

import me.bayee.internetsecurity.pojo.{HttpTrafficLog, ModelInput}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._

import scala.xml.XML

/**
  * Created by mofan on 16-9-27.
  */
object AttactDetectionModel extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("attact_detection_model.xml"))
    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val input = sc
      .newAPIHadoopFile((xml \ "input").text, classOf[AvroKeyInputFormat[GenericRecord]], classOf[AvroKey[GenericRecord]], classOf[NullWritable])
      .map(_._1.datum.toString.parseJson.convertTo[HttpTrafficLog])

    // start the model
    val base = input.filter(_.http_code.getOrElse(-1) == 200)

    (xml \ "rules" \ "rule").foreach { node =>
      base.filter { htl =>
        val split = htl.uri.getOrElse("").split("\\?")
        val getParam = if (split.size > 1) split.drop(1).mkString("?") else ""
        (node \ "regex").text.r.findFirstIn(URLDecoder.decode(getParam, "utf-8")).isDefined || (node \ "regex").text.r.findFirstIn(htl.query_param.getOrElse("")).isDefined
      }
        .map(_.toModelInput.toKeyValueWithId((xml \ "id").text))
        .saveAsSequenceFile((xml \ "hdfsPath").text)
    }
  }
}