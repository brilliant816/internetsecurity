package me.bayee.internetsecurity.r

import me.bayee.internetsecurity.pojo.{HttpTrafficLog, RModelParam}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._
import me.bayee.internetsecurity.util.StringUtil._
import scala.xml.XML

/**
  * Created by mofan on 16-11-5.
  */
object RPredictOutput extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("r_predict_output.xml"))
    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val inputModel = sc
      .newAPIHadoopFile((xml \ "inputModel").text, classOf[AvroKeyInputFormat[GenericRecord]], classOf[AvroKey[GenericRecord]], classOf[NullWritable])
      .map(_._1.datum.toString.parseJson.convertTo[RModelParam])
      .distinct()
      .map(rmp => ((rmp.table_name, rmp.param_key, rmp.param_value), 1))

    sc
      .newAPIHadoopFile((xml \ "input").text + args(0), classOf[AvroKeyInputFormat[GenericRecord]], classOf[AvroKey[GenericRecord]], classOf[NullWritable])
      .map(_._1.datum.toString.parseJson.convertTo[HttpTrafficLog])
      .distinct()
      .map { htl =>
        val uri = htl.uri.getOrElse("").base64Decode.urlDecode
        val dns = uri.getUrl
        val getParams = uri.getParams
        val postParams = if (htl.query_param.isDefined && htl.query_param.get.nonEmpty) htl.query_param.get.base64Decode.postParams
        else List.empty[(String, String)]
        (dns, getParams ::: postParams, htl)
      }
      .filter(_._2.nonEmpty)
      .flatMap {
        case (dns, params, htl) => params.map { kv => ((dns, kv._1, kv._2), htl) }
      }
      .leftOuterJoin(inputModel)
      .filter(_._2._2.isDefined)
      .map(_._2._1.toModelInput)
      .distinct()
      .map(_.toKeyValueWithId((xml \ "id").text))
      .saveAsSequenceFile((xml \ "hdfsPath").text)

  }
}
