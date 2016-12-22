package me.bayee.internetsecurity.pojo

import spray.json.DefaultJsonProtocol

/**
  * Created by mofan on 16-10-23.
  */
object HttpTrafficLog extends DefaultJsonProtocol with BasicPojo{
  implicit val _format = jsonFormat18(apply)
  val key = "HttpTrafficLog"
  val schemaPath = "schema/HttpTrafficLog.avsc"
  val schema = buildSchema(HttpTrafficLog)
  val schemaMap = Map((key,schema))
}

case class HttpTrafficLog(visit_time: Option[String],
                          warden: Option[String],
                          http_code: Option[Int],
                          client_c_address: Option[String],
                          server_ip: Option[String],
                          refer_info: Option[String],
                          server_port: Option[Int],
                          client_request_size: Option[Long],
                          server_response_size: Option[Long],
                          dns: Option[String],
                          cookie: Option[String],
                          client_ip: Option[String],
                          user_agent: Option[String],
                          uri: Option[String],
                          query_param: Option[String],
                          http_method: Option[String],
                          server_c_address: Option[String],
                          httplog: Option[String]) {
  override def toString: String = "\"" + this.productIterator.map(_.asInstanceOf[Option[_ >: Any]].getOrElse("")).mkString("\",\"") + "\""

  def toModelInput: ModelInput = ModelInput(visit_time, client_ip, server_ip, uri, http_code, refer_info, http_method, user_agent, cookie)
}