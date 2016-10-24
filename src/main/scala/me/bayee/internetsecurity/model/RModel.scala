package me.bayee.internetsecurity.model

import java.net.URLDecoder

import me.bayee.internetsecurity.pojo.{HttpTrafficLog, RequestParam}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._
import me.bayee.internetsecurity.util.AvroUtil._
import org.apache.hadoop.mapreduce.Job

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
    AvroJob.setOutputKeySchema(job, RequestParam.schema)

    val tuple1 = sc
      .newAPIHadoopFile((xml \ "input").text, classOf[AvroKeyInputFormat[GenericRecord]], classOf[AvroKey[GenericRecord]], classOf[NullWritable])
      .map(_._1.datum.toString.parseJson.convertTo[HttpTrafficLog])
      .map { htl =>
        val uri = URLDecoder.decode(htl.uri.getOrElse(""), "utf-8")
        val index1 = uri.indexOf("?")
        val index2 = uri.indexOf("/")
        val cut = if (index1 < index2) index1 else index2
        val dns = uri.substring(0, cut)
        val getParams: List[(String, String)] = if (index1 < 0) List.empty[(String, String)]
        else uri.substring(index1 + 1).split("&").map { kv =>
          val s = kv.split("=")
          (s(0), s(1))
        }.toList
        val postParams: List[(String, String)] = if (htl.query_param.isDefined) htl.query_param.get.split("&").map { kv =>
          val s = kv.split("=")
          (s(0), s(1))
        }.toList
        else List.empty[(String, String)]
        (dns, getParams ::: postParams)
      }.filter(_._2.nonEmpty)
      .groupByKey()

    val dnsList = tuple1.map(_._1).toLocalIterator.toList

    val tuple2 = tuple1
      .map {
        case (dns, iter) =>
          (dns,
            iter
              .flatMap(_.toList)
              .toList
              .groupBy(_._1)
              .map(_._2.take(2000))
              .flatMap(_.toList))

      }
    dnsList.foreach { d =>
      tuple2.filter(_._1 == d)
        .flatMap(_._2.toList)
        .map(kv => (new AvroKey(RequestParam(kv._1, kv._2).toJson.asJsObject.convert2GenericRecord(RequestParam.key, RequestParam.schemaMap)), null))
        .saveAsNewAPIHadoopFile((xml \ "output").text, classOf[GenericRecord], classOf[NullWritable], classOf[AvroKeyOutputFormat[GenericRecord]], job.getConfiguration)

    }
  }
}