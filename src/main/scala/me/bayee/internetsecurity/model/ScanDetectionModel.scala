package me.bayee.internetsecurity.model

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import me.bayee.internetsecurity.pojo.{HttpTrafficLog, ModelInput}
import org.apache.spark.{SparkConf, SparkContext}

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
      .textFile((xml \ "input").text)
      .map(ModelInput.fromHttpTrafficLog)

    // useragent feature
    val blackList: List[String] = (xml \ "userAgent" \ "blackList" \ "value").map(_.text).toList
    val base = input
      .filter(mi => blackList.foldLeft(false) { (last, value) => last match {
        case true => true
        case false => mi.userAgent.getOrElse("").contains(value)
      }
      })

    (xml \ "rules" \ "rule").foreach { node =>
      (node \ "id").text match {
        case "301" | "302" => //java代码注入 | 任意文件读取
          base.filter(mi => (node \ "regex").text.r.findFirstIn(mi.url.getOrElse("")).isDefined)
            .map(_.toKeyValueWithId((node \ "id").text))
            .saveAsSequenceFile((node \ "hdfsPath").text)

        case "303" => // 404阀值规则
          base
            .filter(_.httpCode.getOrElse(-1) == 404)
            .map(mi => (mi.url.getOrElse(""), mi))
            .groupByKey()
            .mapPartitions { iter =>
              val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
              iter.map { tuple =>
                val list = tuple._2.toList
                if (list.size <= (node \ "threshold" \ "count").text.toInt) (list, -1)
                else {
                  val dateList = list
                    .map(mi => (sdf.parse(mi.visitTime.get), mi))
                    .sortWith((a, b) => a._1.before(b._1))
                  val count = rollingTimeMaxCount(dateList, (node \ "threshold" \ "seconds").text.toInt)
                  (list, count)
                }
              }
            }
            .filter(_._2 > (node \ "threshold" \ "count").text.toInt)
            .flatMap(_._1.map(_.toKeyValueWithId((node \ "id").text)))
            .saveAsSequenceFile((node \ "hdfsPath").text)

        case "304" | "305" => //敏感文件探测规则 | 敏感目录探测规则
          base
            .map(mi => ((node \ "suffix" \ "regex").text.r.findFirstIn(mi.url.getOrElse("")).getOrElse(""), mi))
            .filter(_._1.nonEmpty)
            .groupByKey()
            .mapPartitions { iter =>
              val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
              iter.map { tuple =>
                val list = tuple._2.toList
                if (list.size <= (node \ "threshold" \ "count").text.toInt) (list, -1)
                else {
                  val dateList = list
                    .map(mi => (sdf.parse(mi.visitTime.get), mi))
                    .sortWith((a, b) => a._1.before(b._1))
                  val count = rollingTimeMaxCount(dateList, (node \ "threshold" \ "seconds").text.toInt)
                  (list, count)
                }
              }
            }
            .filter(_._2 > (node \ "threshold" \ "count").text.toInt)
            .flatMap(_._1.map(_.toKeyValueWithId((node \ "id").text)))
            .saveAsSequenceFile((node \ "hdfsPath").text)
      }
    }
  }

  def rollingTimeMaxCount(list: List[(Date, ModelInput)], seconds: Int): Int =
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

object Test extends App {
  override def main(args: Array[String]): Unit = {
    println("(rar|zip)$".r.findFirstIn("abc.rar"))
  }
}