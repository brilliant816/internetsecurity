package me.bayee.internetsecurity.model

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

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
object HitDbDetectionModel extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("hit_db_detection_model.xml"))
    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val input = sc
      .newAPIHadoopFile((xml \ "input").text + args(0), classOf[AvroKeyInputFormat[GenericRecord]], classOf[AvroKey[GenericRecord]], classOf[NullWritable])
      .map(_._1.datum.toString.parseJson.convertTo[HttpTrafficLog])
      .distinct()

    (xml \ "rules" \ "rule").foreach { node =>
      input
        .map(htl => ((node \ "loginRegex").text.r.findFirstIn(htl.uri.getOrElse("").base64Decode.urlDecode).getOrElse(""), htl))
        .filter(_._1.nonEmpty)
        .groupByKey()
        .mapPartitions { iter =>
          val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
          iter.map { tuple =>
            val dateList = tuple._2.toList.map(htl => (sdf.parse(htl.visit_time.get), htl))
              .sortWith((a, b) => a._1.before(b._1))
            val total = rollingTimeLogin(dateList, (node \ "seconds").text.toInt, (node \ "count").text.toInt)
            if (total.nonEmpty) total
            else {
              val failedList = dateList.filter(htl => (node \ "failureRegex").text.r.findFirstIn(htl._2.uri.getOrElse("").base64Decode.urlDecode).isDefined)
              rollingTimeLogin(failedList, (node \ "seconds").text.toInt, (node \ "failureCount").text.toInt)
            }
          }
        }
        .filter(_.nonEmpty)
        .flatMap(_.map(_.toModelInput.toKeyValueWithId((node \ "id").text)))
        .saveAsSequenceFile((node \ "hdfsPath").text)
    }
  }

  def rollingTimeLogin(list: List[(Date, HttpTrafficLog)], seconds: Int, threshold: Int): List[HttpTrafficLog] =
    if (list.isEmpty) List.empty[HttpTrafficLog]
    else if (list.size <= threshold) List.empty[HttpTrafficLog]
    else {
      var _list = list
      var result = List.empty[HttpTrafficLog]
      while(_list.size > threshold && result.isEmpty){
        val calendar = Calendar.getInstance
        calendar.setTime(_list.head._1)
        calendar.add(Calendar.SECOND,seconds)
        val endDate = calendar.getTime
        val count = _list.lastIndexWhere(_._1.before(endDate))
        if(count +1 > threshold) result = _list.take(count).map(_._2)
        _list = _list.tail
      }
      result
    }
}
