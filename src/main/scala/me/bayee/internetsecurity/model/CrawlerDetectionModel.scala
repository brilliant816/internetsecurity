package me.bayee.internetsecurity.model

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import me.bayee.internetsecurity.pojo. ModelInput
import org.apache.spark.{SparkConf, SparkContext}
import org.xbill.DNS._

import scala.util.matching.Regex
import scala.xml.XML

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
      .textFile((xml \ "input").text)
      .map(ModelInput.fromHttpTrafficLog)

    (xml \ "rules" \ "rule").foreach { node =>
      (node \ "id").text match {
        case "401" => //域名识别规则
          input
            .filter(mi => (node \ "regex").text.r.findFirstIn(mi.userAgent.getOrElse("").toLowerCase).isDefined)
            .filter(_.clientIp match {
              case Some(ip) => (node \ "regex").text.r.findFirstIn(getHostname(ip).toLowerCase).isEmpty
              case None => true
            })
            .map(_.toKeyValueWithId((node \ "id").text))
            .saveAsSequenceFile((node \ "hdfsPath").text)
        case "402" => //高频规则
          input
            .map(mi => (mi.clientIp.getOrElse(""), mi))
            .groupByKey()
            .mapPartitions { iter =>
              val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
              iter.map { tuple =>
                val list = tuple._2.toList
                val count = if (list.size <= (node \ "global" \ "count").text.toInt) -1
                else {
                  val dateList = list.map(mi => (sdf.parse(mi.visitTime.get), mi))
                    .sortWith((a, b) => a._1.before(b._1))
                  rollingTimeMaxCount(dateList, (node \ "global" \ "seconds").text.toInt)
                }

                if (count > (node \ "global" \ "count").text.toInt) (list, count)
                else {
                  (node \ "urls" \ "element").map { urlNode =>
                    val dateList = list
                      .filter(_.url.getOrElse("").contains((urlNode \ "url").text))
                      .map(mi => (sdf.parse(mi.visitTime.get), mi))
                      .sortWith((a, b) => a._1.before(b._1))
                    val c = rollingTimeMaxCount(dateList, (urlNode \ "seconds").text.toInt)
                    if (c > (urlNode \ "count").text.toInt) (list, c)
                    else (list, -1)
                  }.foldLeft((List.empty[ModelInput], 0)) { (last, cur) => if (cur._2 > 0) (last._1 ::: cur._1, last._2 + cur._2) else last }
                }
              }
            }
            .filter(_._2 > 0)
            .flatMap(_._1.map(_.toKeyValueWithId((node \ "id").text)))
            .saveAsSequenceFile((node \ "hdfsPath").text)
        case "403" => //页面比例规则
          input
            .map(mi => (mi.clientIp.getOrElse(""), mi))
            .groupByKey()
            .mapPartitions { iter =>
              val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
              iter.map { tuple =>
                //不特定页面url
                val dateList = tuple._2.toList.map(mi => (sdf.parse(mi.visitTime.get), mi))
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
            .flatMap(_.map(_.toKeyValueWithId((node \ "id").text)))
            .saveAsSequenceFile((node \ "hdfsPath").text)

        case "404" => //userAgent特征规则
          input
            .filter(mi => (node \ "regex").text.r.findFirstIn(mi.userAgent.getOrElse("")).isDefined)
            .map(_.toKeyValueWithId((node \ "id").text))
            .saveAsSequenceFile((node \ "hdfsPath").text)

        case "405" => //http请求包大小规则
          input
            .map(mi => (mi.clientIp.getOrElse(""), mi))
            .groupByKey()
            .mapPartitions { iter =>
              val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
              iter.map { tuple =>
                val dateList = tuple._2.toList.map(mi => (sdf.parse(mi.visitTime.get), mi))
                  .sortWith((a, b) => a._1.before(b._1))
                rollingTimeRequestSizeThreshold(dateList, (node \ "seconds").text.toInt, (node \ "requestSize").text.toInt, (node \ "requestCount").text.toInt)
              }
            }
            .filter(_.nonEmpty)
            .flatMap(_.map(_.toKeyValueWithId((node \ "id").text)))
            .saveAsSequenceFile((node \ "hdfsPath").text)

        case "406" => //静态文件比例规则
          input
            .map(mi => (mi.clientIp.getOrElse(""), mi))
            .groupByKey()
            .mapPartitions { iter =>
              val sdf = new SimpleDateFormat(ModelInput.DATE_FORMAT)
              iter.map { tuple =>
                val dateList = tuple._2.toList.map(mi => (sdf.parse(mi.visitTime.get), mi))
                  .sortWith((a, b) => a._1.before(b._1))
                rollingTimeRegexThreshold(dateList, (node \ "regex").text.r, (node \ "seconds").text.toInt, (node \ "matchCount").text.toInt, (node \ "totalCount").text.toInt)
              }
            }
            .filter(_.nonEmpty)
            .flatMap(_.map(_.toKeyValueWithId((node \ "id").text)))
            .saveAsSequenceFile((node \ "hdfsPath").text)
      }
    }
  }

  def rollingTimeRegexThreshold(list: List[(Date, ModelInput)], regex: Regex, seconds: Int, matchCount: Int, totalCount: Int): List[ModelInput] =
    if (list.isEmpty) List.empty[ModelInput]
    else if (list.size <= totalCount) List.empty[ModelInput]
    else {
      val result = rollingTimeRegexThreshold(list.tail, regex, seconds, matchCount, totalCount)
      if (result.nonEmpty) result
      else {
        val calender = Calendar.getInstance()
        calender.setTime(list.head._1)
        calender.add(Calendar.SECOND, seconds)
        val endDate = calender.getTime
        val subList = list.takeWhile(_._1.before(endDate))
        val count = subList.count(mi => regex.findFirstIn(mi._2.url.getOrElse("")).isDefined)
        if (count < matchCount && subList.size > totalCount) subList.map(_._2)
        else List.empty[ModelInput]
      }
    }

  def rollingTimeRequestSizeThreshold(list: List[(Date, ModelInput)], seconds: Int, requestSize: Int, requestCount: Int): List[ModelInput] =
    if (list.isEmpty) List.empty[ModelInput]
    else {
      val result = rollingTimeRequestSizeThreshold(list.tail, seconds, requestSize, requestCount)
      if (result.nonEmpty) result
      else {
        val calender = Calendar.getInstance()
        calender.setTime(list.head._1)
        calender.add(Calendar.SECOND, seconds)
        val endDate = calender.getTime
        val subList = list.takeWhile(_._1.before(endDate))
        val size = subList.map(_._2.clientRequestSize.getOrElse[Long](0)).sum
        if (size < requestSize && subList.size > requestCount) subList.map(_._2)
        else List.empty[ModelInput]
      }
    }

  def rollingTimeUrlThreshold(list: List[(Date, ModelInput)], url: String, seconds: Int, percent: Double): List[ModelInput] =
    if (list.isEmpty) List.empty[ModelInput]
    else {
      val result = rollingTimeUrlThreshold(list.tail, url, seconds, percent)
      if (result.nonEmpty) result
      else {
        val calendar = Calendar.getInstance()
        calendar.setTime(list.head._1)
        calendar.add(Calendar.SECOND, seconds)
        val endDate = calendar.getTime
        val subList = list.takeWhile(_._1.before(endDate))
        val count = subList.count(_._2.url.getOrElse("").contains(url))
        if (count.toDouble / subList.size.toDouble > percent) subList.map(_._2)
        else List.empty[ModelInput]
      }
    }

  def rollingTimeThreshold(list: List[(Date, ModelInput)], seconds: Int, upBound: Int, lowBound: Int): List[ModelInput] =
    if (list.isEmpty) List.empty[ModelInput]
    else if (list.size <= upBound) List.empty[ModelInput]
    else {
      val result = rollingTimeThreshold(list.tail, seconds, upBound, lowBound)
      if (result.nonEmpty) result
      else {
        val calender = Calendar.getInstance()
        calender.setTime(list.head._1)
        calender.add(Calendar.SECOND, seconds)
        val endDate = calender.getTime
        val subList = list.takeWhile(_._1.before(endDate))
        val up = subList.size
        val low = subList.map(_._2.url.getOrElse("")).distinct.size
        if (up > upBound && low < lowBound) subList.map(_._2) else List.empty[ModelInput]
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

  def getHostname(ip: String) = {
    val res = new ExtendedResolver()
    val rec = Record.newRecord(ReverseMap.fromAddress(ip), Type.PTR, DClass.IN)
    val query = Message.newQuery(rec)
    val response = res.send(query)
    val answers = response.getSectionArray(Section.ANSWER)
    if (answers.isEmpty) ip
    else answers(0).rdataToString()
  }
}
