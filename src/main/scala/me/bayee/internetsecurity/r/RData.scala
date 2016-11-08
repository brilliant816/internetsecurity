package me.bayee.internetsecurity.r

import me.bayee.internetsecurity.pojo.{HttpTrafficLog, RModelParam}
import me.bayee.internetsecurity.util.AvroUtil._
import me.bayee.internetsecurity.util.StringUtil._
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._

import scala.xml.XML

/**
  * Created by mofan on 16-10-24.
  */
object RData extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("r_data.xml"))
    val dir = args(0)
    val mode = args(1) // training or predict

    val input = if (mode == "training") (xml \ "training" \ "input").text + dir else (xml \ "predict" \ "input").text + dir
    val hdfsPath = if (mode == "training") (xml \ "training" \ "hdfsPath").text else (xml \ "predict" \ "hdfsPath").text



    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val job = new Job(sc.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, RModelParam.schema)

    val pipe = sc
      .newAPIHadoopFile(input, classOf[AvroKeyInputFormat[GenericRecord]], classOf[AvroKey[GenericRecord]], classOf[NullWritable])
      .map(_._1.datum.toString.parseJson.convertTo[HttpTrafficLog])
      .distinct()
      .map { htl =>
        val uri = htl.uri.getOrElse("").base64Decode.urlDecode
        val dns = uri.getUrl
        val getParams: List[(String, String)] = uri.getParams
        val postParams: List[(String, String)] = if (htl.query_param.isDefined && htl.query_param.get.nonEmpty) htl.query_param.get.base64Decode.postParams
        else List.empty[(String, String)]
        (dns, getParams ::: postParams)
      }.filter(_._2.nonEmpty)

    if (mode == "training") {
      pipe
        .groupByKey()
        .flatMap {
          case (key, iterator) => iterator
            .flatMap(_.toList)
            .toList
            .groupBy(_._1)
            .map(_._2.take(2000))
            .filter(x => (xml \ "training" \ "seqLength").text.toInt < x.size)
            .flatMap(_.map(kv => RModelParam(key, kv._1, kv._2)))
            .map(rmp => (new AvroKey(rmp.toJson.asJsObject.convert2GenericRecord(RModelParam.key, RModelParam.schemaMap)), null))
        }.saveAsNewAPIHadoopFile(hdfsPath, classOf[GenericRecord], classOf[NullWritable], classOf[AvroKeyOutputFormat[GenericRecord]], job.getConfiguration)
    } else {
      pipe
        .flatMap {
          case (dns, params) => params.map(kv => RModelParam(dns, kv._1, kv._2))
        }
        .filter(_.param_value.nonEmpty)
        .map(rmp => (new AvroKey(rmp.toJson.asJsObject.convert2GenericRecord(RModelParam.key, RModelParam.schemaMap)), null))
        .saveAsNewAPIHadoopFile(hdfsPath, classOf[GenericRecord], classOf[NullWritable], classOf[AvroKeyOutputFormat[GenericRecord]], job.getConfiguration)
    }
  }
}