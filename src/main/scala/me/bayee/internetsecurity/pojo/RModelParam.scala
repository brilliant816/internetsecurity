package me.bayee.internetsecurity.pojo

import spray.json.DefaultJsonProtocol

/**
  * Created by mofan on 16-10-24.
  */
object RModelParam extends DefaultJsonProtocol with BasicPojo{
  implicit val _format = jsonFormat3(apply)
  val key = "r_model_param"
  val schemaPath = "schema/r_model_param.avsc"
  val schema = buildSchema(RModelParam)
  val schemaMap = Map((key,schema))
}

case class RModelParam(table_name : String, param_key: String, param_value: String)