package me.bayee.internetsecurity.pojo

import java.security.MessageDigest

/**
  * Created by mofan on 16-10-15.
  */
case class ModelInput(visitTime: Option[String],
                      clientIp: Option[String],
                      serverIp: Option[String],
                      url: Option[String],
                      httpCode: Option[Int],
                      referInfo: Option[String],
                      httpMethod: Option[String],
                      userAgent: Option[String],
                      cookie: Option[String],
                      clientRequestSize: Option[Long],
                      serverResponseSize: Option[Long]
                     ) {
  def toKeyValueWithId(id: String) = {
    val line = "\"" + this.productIterator.map(_.asInstanceOf[Option[_ >: Any]].getOrElse("")).mkString("\",\"") + "\""
    val key = s"${visitTime.getOrElse("")}-${new String(MessageDigest.getInstance("MD5").digest(line.getBytes))}"
    val value = line + ",\"" + id + "\""
    (key, value)
  }
}

object ModelInput {
  private implicit def stringSomeOrNone(str: String): Option[String] = if (str.isEmpty) None else Some(str)

  private implicit def intSomeOrNone(str: String): Option[Int] = if (str.isEmpty) None else Some(str.toInt)

  private implicit def longSomeOrNone(str: String): Option[Long] = if (str.isEmpty) None else Some(str.toLong)

  val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

  def fromHttpTrafficLog(line: String) = {
    val split = line.split("\",\"").map(_.replace("\"", ""))
    ModelInput(split(0),
      split(11),
      split(4),
      split(13),
      split(2),
      split(5),
      split(15),
      split(12),
      split(10),
      split(7),
      split(8)
    )
  }

  def fromModelInput(line: String): (ModelInput, String) = {
    val split = line.split("\",\"").map(_.replace("\"", ""))
    (ModelInput(
      split(0),
      split(1),
      split(2),
      split(3),
      split(4),
      split(5),
      split(6),
      split(7),
      split(8),
      split(9),
      split(10)
    ), split(11))
  }
}
