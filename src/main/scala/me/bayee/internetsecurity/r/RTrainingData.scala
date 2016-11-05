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
object RTrainingData extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("r_training_data.xml"))
    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val job = new Job(sc.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, RModelParam.schema)

    sc
      .newAPIHadoopFile((xml \ "input").text + args(0), classOf[AvroKeyInputFormat[GenericRecord]], classOf[AvroKey[GenericRecord]], classOf[NullWritable])
      .map(_._1.datum.toString.parseJson.convertTo[HttpTrafficLog])
      .map { htl =>
        val uri = htl.uri.getOrElse("").base64Decode.urlDecode
        val dns = uri.getUrl
        val getParams: List[(String, String)] = uri.getParams
        val postParams: List[(String, String)] = if (htl.query_param.isDefined) htl.query_param.get.base64Decode.postParams
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