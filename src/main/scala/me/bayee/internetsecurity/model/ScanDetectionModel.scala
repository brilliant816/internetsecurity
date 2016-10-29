package me.bayee.internetsecurity.model

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import me.bayee.internetsecurity.util.StringUtil._
import me.bayee.internetsecurity.pojo.{HttpTrafficLog, ModelInput}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._

import scala.xml.XML

/**
  * Created by mofan on 16-10-1.
  */
object ScanDetectionModel extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("scan_detection_model.xml"))
    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val input = sc
      .newAPIHadoopFile((xml \ "input").text, classOf[AvroKeyInputFormat[GenericRecord]], classOf[AvroKey[GenericRecord]], classOf[NullWritable])
      .map(_._1.datum.toString.parseJson.convertTo[HttpTrafficLog])

    // useragent feature
    val blackList: List[String] = (xml \ "userAgent" \ "blackList" \ "value").map(_.text).toList
    val base = input
      .filter(htl => blackList.foldLeft(false) { (last, value) => last match {
        case true => true
        case false => htl.user_agent.getOrElse("").contains(value)
      }
      })

    (xml \ "rules" \ "rule").foreach { node =>
      (node \ "id").text match {
        case "301" | "302" => //java代码注入 | 任意文件读取
          base.filter{htl =>
            (node \ "regex").text.r.findFirstIn(htl.uri.getOrElse("").base64Decode.urlDecode.getParamString).isDefined || (node \ "regex").text.r.findFirstIn(htl.query_param.getOrElse("").base64Decode).isDefined}
            .map(_.toModelInput.toKeyValueWithId((node \ "id").text))
            .saveAsSequenceFile((node \ "hdfsPath").text)

        case "303" => // 404阀值规则
          base
            .filter(_.http_code.getOrElse(-1) == 404)
            .map(htl => (htl.uri.getOrElse("").base64Decode.getUrl, htl))
            .groupByKey()
            .mapPartitions { iter =>
              val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
              iter.map { tuple =>
                val list = tuple._2.toList
                if (list.size <= (node \ "threshold" \ "count").text.toInt) (list, -1)
                else {
                  val dateList = list
                    .map(htl => (sdf.parse(htl.visit_time.get), htl))
                    .sortWith((a, b) => a._1.before(b._1))
                  val count = rollingTimeMaxCount(dateList, (node \ "threshold" \ "seconds").text.toInt)
                  (list, count)
                }
              }
            }
            .filter(_._2 > (node \ "threshold" \ "count").text.toInt)
            .flatMap(_._1.map(_.toModelInput.toKeyValueWithId((node \ "id").text)))
            .saveAsSequenceFile((node \ "hdfsPath").text)

        case "304" | "305" => //敏感文件探测规则 | 敏感目录探测规则
          base
            .map(htl => ((node \ "regex").text.r.findFirstIn(htl.uri.getOrElse("").base64Decode).getOrElse(""), htl))
            .filter(_._1.nonEmpty)
            .groupByKey()
            .mapPartitions { iter =>
              val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
              iter.map { tuple =>
                val list = tuple._2.toList
                if (list.size <= (node \ "threshold" \ "count").text.toInt) (list, -1)
                else {
                  val dateList = list
                    .map(htl => (sdf.parse(htl.visit_time.get), htl))
                    .sortWith((a, b) => a._1.before(b._1))
                  val count = rollingTimeMaxCount(dateList, (node \ "threshold" \ "seconds").text.toInt)
                  (list, count)
                }
              }
            }
            .filter(_._2 > (node \ "threshold" \ "count").text.toInt)
            .flatMap(_._1.map(_.toModelInput.toKeyValueWithId((node \ "id").text)))
            .saveAsSequenceFile((node \ "hdfsPath").text)
      }
    }
  }

  def rollingTimeMaxCount(list: List[(Date, HttpTrafficLog)], seconds: Int): Int =
    if (list.isEmpty) 0
    else {
      val calendar = Calendar.getInstance
      calendar.setTime(list.head._1)
      calendar.add(Calendar.SECOND, seconds)
      val endDate = calendar.getTime
      val count = list.lastIndexWhere(_._1.before(endDate)) + 1
      val max = rollingTimeMaxCount(list.tail, seconds)
      if (count > max) count else max
    }
}