package me.bayee.internetsecurity.pojo

import spray.json.DefaultJsonProtocol

/**
  * Created by mofan on 16-10-24.
  */
object RequestParam extends DefaultJsonProtocol with BasicPojo{
  implicit val _format = jsonFormat2(apply)
  val key = "request_param"
  val schemaPath = "schema/request_param.avsc"
  val schema = buildSchema(RequestParam)
  val schemaMap = Map((key,schema))
}

case class RequestParam(param_key: String, param_value: String)