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
import org.xbill.DNS._
import spray.json._
import scala.util.matching.Regex
import scala.xml.XML
import me.bayee.internetsecurity.util.RetryUtil._

/**
  * Created by mofan on 16-10-10.
  */
object CrawlerDetectionModel extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("crawler_detection_model.xml"))
    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)


    val input = sc
      .newAPIHadoopFile((xml \ "input").text + args(0), classOf[AvroKeyInputFormat[GenericRecord]], classOf[AvroKey[GenericRecord]], classOf[NullWritable])
      .map(_._1.datum.toString.parseJson.convertTo[HttpTrafficLog])
      .distinct()

    (xml \ "rules" \ "rule").foreach { node =>
      (node \ "id").text match {
        case "401" => //域名识别规则
          input
            .filter(htl => (node \ "regex").text.r.findFirstIn(htl.user_agent.getOrElse("").toLowerCase).isDefined)
            .filter(_.client_ip match {
              case Some(ip) => (node \ "regex").text.r.findFirstIn(getHostname(ip).toLowerCase).isEmpty
              case None => true
            })
            .map(_.toModelInput)
            .distinct()
            .map(_.toKeyValueWithId((node \ "id").text))
            .saveAsSequenceFile((node \ "hdfsPath").text)
        case "402" => //高频规则
          input
            .map(htl => (htl.client_ip.getOrElse(""), htl))
            .groupByKey()
            .mapPartitions { iter =>
              val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
              iter.map { tuple =>
                val list = tuple._2.toList
                val count = if (list.size <= (node \ "global" \ "count").text.toInt) -1
                else {
                  val dateList = list.map(htl => (sdf.parse(htl.visit_time.get), htl))
                    .sortWith((a, b) => a._1.before(b._1))
                  rollingTimeMaxCount(dateList, (node \ "global" \ "seconds").text.toInt)
                }

                if (count > (node \ "global" \ "count").text.toInt) (list, count)
                else {
                  (node \ "urls" \ "element").map { urlNode =>
                    val dateList = list
                      .filter(_.uri.getOrElse("").base64Decode.getUrl.contains((urlNode \ "url").text))
                      .map(htl => (sdf.parse(htl.visit_time.get), htl))
                      .sortWith((a, b) => a._1.before(b._1))
                    val c = rollingTimeMaxCount(dateList, (urlNode \ "seconds").text.toInt)
                    if (c > (urlNode \ "count").text.toInt) (list, c)
                    else (list, -1)
                  }.foldLeft((List.empty[HttpTrafficLog], 0)) { (last, cur) => if (cur._2 > 0) (last._1 ::: cur._1, last._2 + cur._2) else last }
                }
              }
            }
            .filter(_._2 > 0)
            .flatMap(_._1.map(_.toModelInput))
            .distinct()
            .map(_.toKeyValueWithId((node \ "id").text))
            .saveAsSequenceFile((node \ "hdfsPath").text)
        case "403" => //页面比例规则
          input
            .map(htl => (htl.client_ip.getOrElse(""), htl))
            .groupByKey()
            .mapPartitions { iter =>
              val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
              iter.map { tuple =>
                //不特定页面url
                val dateList = tuple._2.toList.map(htl => (sdf.parse(htl.visit_time.get), htl))
                  .sortWith((a, b) => a._1.before(b._1))
                val result1 = rollingTimeThreshold(dateList, (node \ "global" \ "seconds").text.toInt, (node \ "global" \ "upBound").text.toInt, (node \ "global" \ "lowBound").text.toInt)
                //特定页面url
                if (result1.nonEmpty) result1
                else
                  (node \ "urls" \ "element").map { urlNode =>
                    rollingTimeUrlThreshold(dateList, (urlNode \ "url").text, (urlNode \ "seconds").text.toInt, (urlNode \ "percent").text.toDouble)
                  }.flatMap(_.toList)
              }
            }
            .filter(_.nonEmpty)
            .flatMap(_.map(_.toModelInput))
            .distinct()
            .map(_.toKeyValueWithId((node \ "id").text))
            .saveAsSequenceFile((node \ "hdfsPath").text)

        case "404" => //userAgent特征规则
          input
            .filter(htl => (node \ "regex").text.r.findFirstIn(htl.user_agent.getOrElse("")).isDefined)
            .map(_.toModelInput)
            .distinct()
            .map(_.toKeyValueWithId((node \ "id").text))
            .saveAsSequenceFile((node \ "hdfsPath").text)

        case "405" => //http请求包大小规则
          input
            .map(htl => (htl.client_ip.getOrElse(""), htl))
            .groupByKey()
            .mapPartitions { iter =>
              val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
              iter.map { tuple =>
                val dateList = tuple._2.toList.map(htl => (sdf.parse(htl.visit_time.get), htl))
                  .sortWith((a, b) => a._1.before(b._1))
                rollingTimeRequestSizeThreshold(dateList, (node \ "seconds").text.toInt, (node \ "requestSize").text.toInt, (node \ "requestCount").text.toInt)
              }
            }
            .filter(_.nonEmpty)
            .flatMap(_.map(_.toModelInput))
            .distinct()
            .map(_.toKeyValueWithId((node \ "id").text))
            .saveAsSequenceFile((node \ "hdfsPath").text)

        case "406" => //静态文件比例规则
          input
            .map(htl => (htl.client_ip.getOrElse(""), htl))
            .groupByKey()
            .mapPartitions { iter =>
              val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
              iter.map { tuple =>
                val dateList = tuple._2.toList.map(htl => (sdf.parse(htl.visit_time.get), htl))
                  .sortWith((a, b) => a._1.before(b._1))
                rollingTimeRegexThreshold(dateList, (node \ "regex").text.r, (node \ "seconds").text.toInt, (node \ "matchCount").text.toInt, (node \ "totalCount").text.toInt)
              }
            }
            .filter(_.nonEmpty)
            .flatMap(_.map(_.toModelInput))
            .distinct()
            .map(_.toKeyValueWithId((node \ "id").text))
            .saveAsSequenceFile((node \ "hdfsPath").text)
      }
    }
  }

  def rollingTimeRegexThreshold(list: List[(Date, HttpTrafficLog)], regex: Regex, seconds: Int, matchCount: Int, totalCount: Int): List[HttpTrafficLog] =
    if (list.isEmpty) List.empty[HttpTrafficLog]
    else if (list.size <= totalCount) List.empty[HttpTrafficLog]
    else {
      var _list = list
      var result = List.empty[HttpTrafficLog]
      while (_list.size > totalCount && result.isEmpty) {
        val calendar = Calendar.getInstance()
        calendar.setTime(_list.head._1)
        calendar.add(Calendar.SECOND, seconds)
        val endDate = calendar.getTime
        val subList = _list.takeWhile(_._1.before(endDate))
        val count = subList.count(htl => regex.findFirstIn(htl._2.uri.getOrElse("").base64Decode.urlDecode).isDefined)
        if (count < matchCount && subList.size > totalCount) result = subList.map(_._2)
        _list = _list.tail
      }
      result
    }

  def rollingTimeRequestSizeThreshold(list: List[(Date, HttpTrafficLog)], seconds: Int, requestSize: Int, requestCount: Int): List[HttpTrafficLog] =
    if (list.isEmpty) List.empty[HttpTrafficLog]
    else if (list.size <= requestCount) List.empty[HttpTrafficLog]
    else {
      var _list = list
      var result = List.empty[HttpTrafficLog]
      while (_list.size > requestCount && result.isEmpty) {
        val calendar = Calendar.getInstance()
        calendar.setTime(_list.head._1)
        calendar.add(Calendar.SECOND, seconds)
        val endDate = calendar.getTime
        val subList = _list.takeWhile(_._1.before(endDate))
        val size = subList.map(_._2.client_request_size.getOrElse[Long](0)).sum
        if (size != 0 && size < requestSize && subList.size > requestCount) result = subList.map(_._2)
        _list = _list.tail
      }
      result
    }

  def rollingTimeUrlThreshold(list: List[(Date, HttpTrafficLog)], url: String, seconds: Int, percent: Double): List[HttpTrafficLog] =
    if (list.isEmpty) List.empty[HttpTrafficLog]
    else {
      var _list = list
      var result = List.empty[HttpTrafficLog]
      while (result.isEmpty && _list.nonEmpty) {
        val calendar = Calendar.getInstance()
        calendar.setTime(_list.head._1)
        calendar.add(Calendar.SECOND, seconds)
        val endDate = calendar.getTime
        val subList = _list.takeWhile(_._1.before(endDate))
        val count = subList.count(_._2.uri.getOrElse("").base64Decode.urlDecode.contains(url))
        if (count.toDouble / subList.size.toDouble > percent) result = subList.map(_._2)
        _list = _list.tail
      }
      result
    }

  def rollingTimeThreshold(list: List[(Date, HttpTrafficLog)], seconds: Int, upBound: Int, lowBound: Int): List[HttpTrafficLog] =
    if (list.isEmpty) List.empty[HttpTrafficLog]
    else if (list.size <= upBound) List.empty[HttpTrafficLog]
    else {
      var _list = list
      var result = List.empty[HttpTrafficLog]
      while (result.isEmpty && _list.size > upBound) {
        val calender = Calendar.getInstance()
        calender.setTime(_list.head._1)
        calender.add(Calendar.SECOND, seconds)
        val endDate = calender.getTime
        val subList = _list.takeWhile(_._1.before(endDate))
        val up = subList.size
        val low = subList.map(_._2.uri.getOrElse("").base64Decode.getUrl).distinct.size
        if (up > upBound && low < lowBound) result = subList.map(_._2)
        _list = _list.tail
      }
      result
    }

  def rollingTimeMaxCount(list: List[(Date, HttpTrafficLog)], seconds: Int): Int =
    if (list.isEmpty) 0
    else {
      var _list = list
      var max = -1
      while (_list.nonEmpty && _list.size > max) {
        val calendar = Calendar.getInstance
        calendar.setTime(_list.head._1)
        calendar.add(Calendar.SECOND, seconds)
        val endDate = calendar.getTime
        val count = _list.lastIndexWhere(_._1.before(endDate)) + 1
        if (max < count) max = count
        _list = _list.tail
      }
      max
    }

  def getHostname(ip: String): String = retry[String](5) {
    val res = new ExtendedResolver()
    val rec = Record.newRecord(ReverseMap.fromAddress(ip), Type.PTR, DClass.IN)
    val query = Message.newQuery(rec)
    val response = res.send(query)
    val answers = response.getSectionArray(Section.ANSWER)
    if (answers.isEmpty) ip
    else answers(0).rdataToString()
  }
}