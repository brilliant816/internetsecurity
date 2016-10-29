package me.bayee.internetsecurity.model

import me.bayee.internetsecurity.pojo.{HttpTrafficLog, ModelInput}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._
import me.bayee.internetsecurity.util.StringUtil._
import scala.xml.XML

/**
  * Created by mofan on 16-9-18.
  */
object GraphDetectionModel extends App {

  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("graph_detection_model.xml"))
    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val input = sc
      .newAPIHadoopFile((xml \ "input").text, classOf[AvroKeyInputFormat[GenericRecord]], classOf[AvroKey[GenericRecord]], classOf[NullWritable])
      .map(_._1.datum.toString.parseJson.convertTo[HttpTrafficLog])

    // start the model
    val inDegreeZero = input.filter(_.refer_info.isEmpty)
    val filterUrl = inDegreeZero
      .map(htl => (htl.uri.getOrElse("").getUrl, htl.client_ip.getOrElse("")))
      .distinct
      .map(x => (x._1, 1))
      .reduceByKey(_ + _)
      .filter(_._2 <= 10)

    val base = inDegreeZero
      .map(htl => (htl.uri.getOrElse("").getUrl, htl))
      .leftOuterJoin(filterUrl)
      .filter(_._2._2.isDefined)
      .map(_._2._1)

    (xml \ "rules" \ "rule").foreach { node =>
      base.filter { htl =>
        (node \ "regex").text.r.findFirstIn(htl.uri.getOrElse("").base64Decode).isDefined || (node \ "regex").text.r.findFirstIn(htl.query_param.getOrElse("").base64Decode).isDefined
      }
        .map(_.toModelInput.toKeyValueWithId((node \ "id").text))
        .saveAsSequenceFile((node \ "hdfsPath").text)
    }
  }
}