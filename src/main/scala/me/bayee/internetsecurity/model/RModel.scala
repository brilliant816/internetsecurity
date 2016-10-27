package me.bayee.internetsecurity.model

import java.net.URLDecoder

import me.bayee.internetsecurity.pojo.{HttpTrafficLog, RModelParam}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._
import me.bayee.internetsecurity.util.AvroUtil._
import org.apache.hadoop.mapreduce.Job
import sun.misc.BASE64Decoder

import scala.xml.XML

/**
  * Created by mofan on 16-10-24.
  */
object RModel extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("r_model.xml"))
    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val job = new Job(sc.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, RModelParam.schema)

    sc
      .newAPIHadoopFile((xml \ "input").text, classOf[AvroKeyInputFormat[GenericRecord]], classOf[AvroKey[GenericRecord]], classOf[NullWritable])
      .map(_._1.datum.toString.parseJson.convertTo[HttpTrafficLog])
      .map { htl =>
        val uri = if(htl.uri.getOrElse("").startsWith("[BASE64-DATA]")) {
          val decoder = new BASE64Decoder()
          new String(decoder.decodeBuffer(htl.uri.getOrElse("").replace("[BASE64-DATA]","").replace("[/BASE64-DATA]","")))
        } else {
          URLDecoder.decode(htl.uri.getOrElse(""), "utf-8")
        }
        val index1 = uri.indexOf("?")
        val index2 = uri.indexOf("/")
        val dns = if (index1 < index2 && index1 > 0) uri.substring(0, index1)
        else if (index2 > 0) uri.substring(0, index2)
        else uri
        val getParams: List[(String, String)] = if (index1 < 0) List.empty[(String, String)]
        else uri.substring(index1 + 1).split("&").map { kv =>
          val s = kv.split("=")
          (s(0), s(1))
        }.toList
        val postParams: List[(String, String)] = if (htl.query_param.isDefined && htl.query_param.get.nonEmpty) {
          val queryParam = if(htl.query_param.get.startsWith("[BASE64-DATA]")) {
            val decoder = new BASE64Decoder()
            new String(decoder.decodeBuffer(htl.query_param.get.replace("[BASE64-DATA]","").replace("[/BASE64-DATA]","")))
          }else htl.query_param.get
          queryParam.split("&").map { kv =>
            val s = kv.split("=")
            if (s.size == 1) ("", kv) else (s(0), s(1))
          }.toList
        }
        else List.empty[(String, String)]
        (dns, getParams ::: postParams)
      }.filter(_._2.nonEmpty)
      .groupByKey()
      .flatMap {
        case (key, iterator) => iterator
          .flatMap(_.toList)
          .toList
          .groupBy(_._1)
          .map(_._2.take(2000))
          .flatMap(_.map(kv => RModelParam(key, kv._1, kv._2)))
          .map(rmp => (new AvroKey(rmp.toJson.asJsObject.convert2GenericRecord(RModelParam.key, RModelParam.schemaMap)), null))
      }.saveAsNewAPIHadoopFile((xml \ "hdfsPath").text, classOf[GenericRecord], classOf[NullWritable], classOf[AvroKeyOutputFormat[GenericRecord]], job.getConfiguration)
  }
}