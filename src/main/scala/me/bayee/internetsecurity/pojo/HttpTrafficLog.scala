package me.bayee.internetsecurity.pojo

import java.security.MessageDigest

/**
  * Created by mofan on 16-9-30.
  */
case class HttpTrafficLog(
                           visitTime: Option[String],
                           warden: Option[String],
                           httpCode: Option[Int],
                           clientCAddress: Option[String],
                           serverIp: Option[String],
                           referInfo: Option[String],
                           serverPort: Option[String],
                           clientRequestSize: Option[Long],
                           serverResponseSize: Option[Long],
                           dns: Option[String],
                           cookie: Option[String],
                           clientIp: Option[String],
                           userAgent: Option[String],
                           uri: Option[String],
                           queryParam: Option[String],
                           httpMethod: Option[String],
                           serverCAddress: Option[String],
                           httplog: Option[String]
                         ) {
  def toKeyValueWithScore(score: String) = {
    val line = "\"" + this.productIterator.map(_.asInstanceOf[Option[_ >: Any]].getOrElse("")).mkString("\",\"") + "\""
    val key = s"$visitTime-${new String(MessageDigest.getInstance("MD5").digest(line.getBytes))}"
    val value = line + ",\"" + score + "\""
    (key, value)
  }
}

object HttpTrafficLog {
  private implicit def stringSomeOrNone(str: String): Option[String] = if (str.isEmpty) None else Some(str)

  private implicit def intSomeOrNone(str: String): Option[Int] = if (str.isEmpty) None else Some(str.toInt)

  private implicit def longSomeOrNone(str: String): Option[Long] = if (str.isEmpty) None else Some(str.toLong)

  def fromLine(line: String) = {
    val split = line.split("\",\"").map(_.replace("\"", ""))
    HttpTrafficLog(split(0),
      split(1),
      split(2),
      split(3),
      split(4),
      split(5),
      split(6),
      split(7),
      split(8),
      split(9),
      split(10),
      split(11),
      split(12),
      split(13),
      split(14),
      split(15),
      split(16),
      split(17))
  }
}