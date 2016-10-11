package me.bayee.internetsecurity.model

import me.bayee.internetsecurity.pojo.HttpTrafficLog
import org.apache.spark.{SparkConf, SparkContext}
import org.xbill.DNS._

import scala.xml.XML

/**
  * Created by mofan on 16-10-10.
  */
object CrawlerDetectionModel extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("crawler_detection_model.xml"))
    val conf = new SparkConf().setAppName("CrawlerDetectionModel").setMaster("local[8]")
    val sc = new SparkContext(conf)


    val input = sc
      .textFile("/home/mofan/Documents/guoxiong/sanxilog/2016-01-27-30.log")
      .map(HttpTrafficLog.fromLine)

    (xml \ "rules" \ "rule").foreach{node =>
      (node \ "id").text match {
        case "1" => //域名识别规则
          val whiteList = (node \ "whiteList").map(_.text.toLowerCase).toList
          input
            .filter(_.userAgent match {
              case Some(userAgent) => whiteList.foldLeft(false) { (bool, cur) => if (bool) bool else userAgent.toLowerCase.contains(cur) }
              case None => false
            })
            .filter(_.clientIp match {
              case Some(ip) =>
                val hostname = getHostname(ip)
                whiteList.foldLeft(true) { (bool, cur) => if (bool) !hostname.toLowerCase.contains(cur) else bool }
              case None => true
            })
            .map(_.toKeyValueWithScore((node \ "score").text))
            .saveAsSequenceFile((node \ "hdfsPath").text)
        case "2" => //高频规则

      }
    }
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
