package me.bayee.internetsecurity.model

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import me.bayee.internetsecurity.pojo.ModelInput
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.XML

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
      .textFile((xml \ "input").text)
      .map(ModelInput.fromHttpTrafficLog)

    (xml \ "rules" \ "rule").foreach { node =>
      input
        .map(mi => ((node \ "loginRegex").text.r.findFirstIn(mi.url.getOrElse("")).getOrElse(""), mi))
        .filter(_._1.nonEmpty)
        .groupByKey()
        .mapPartitions { iter =>
          val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
          iter.map { tuple =>
            val dateList = tuple._2.toList.map(mi => (sdf.parse(mi.visitTime.get), mi))
              .sortWith((a, b) => a._1.before(b._1))
            val total = rollingTimeLogin(dateList, (node \ "seconds").text.toInt, (node \ "count").text.toInt)
            if (total.nonEmpty) total
            else {
              val failedList = dateList.filter(mi => (node \ "failureRegex").text.r.findFirstIn(mi._2.url.getOrElse("")).isDefined)
              rollingTimeLogin(failedList, (node \ "seconds").text.toInt, (node \ "failureCount").text.toInt)
            }
          }
        }
        .filter(_.nonEmpty)
        .flatMap(_.map(_.toKeyValueWithId((node \ "id").text)))
        .saveAsSequenceFile((node \ "hdfsPath").text)
    }
  }

  def rollingTimeLogin(list: List[(Date, ModelInput)], seconds: Int, threshold: Int): List[ModelInput] =
    if (list.isEmpty) List.empty[ModelInput]
    else if (list.size <= threshold) List.empty[ModelInput]
    else {
      val result = rollingTimeLogin(list.tail, seconds, threshold)
      if (result.nonEmpty) result
      else {
        val calendar = Calendar.getInstance
        calendar.setTime(list.head._1)
        calendar.add(Calendar.SECOND, seconds)
        val endDate = calendar.getTime
        val count = list.lastIndexWhere(_._1.before(endDate))
        if (count + 1 > threshold) list.take(count).map(_._2)
        else List.empty[ModelInput]
      }
    }
}
