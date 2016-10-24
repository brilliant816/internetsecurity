package me.bayee.internetsecurity.util

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import spray.json.{JsArray, JsBoolean, JsNumber, JsObject, JsString, JsValue}
import java.util
import scala.collection.JavaConverters._

/**
  * Created by mofan on 16-10-23.
  */
object AvroUtil {
  implicit def buildRichJsObject(jsObject: JsObject) = RichJsObject(jsObject)
}

case class RichJsObject(jsObject: JsObject) {
  def convert2GenericRecord(fieldName: String, schemaMap: Map[String, Schema]) = parse(jsObject, fieldName, schemaMap)

  private def parse(jsValue: JsValue, fieldName: String, schemaMap: Map[String, Schema]): GenericData.Record = jsValue match {
    case _jsObject: JsObject => _jsObject.fields
      .filter { kv =>
        val f = schemaMap(fieldName).getField(kv._1)
        if (f != null) true
        else false
      }
      .foldLeft(new GenericData.Record(schemaMap(fieldName))) {
        case (record, (name, value: JsString)) =>
          record.put(name, value.value)
          record
        case (record, (name, value: JsNumber)) if parseJsNumber(record, name).equalsIgnoreCase("int") =>
          record.put(name, value.value.toInt)
          record
        case (record, (name, value: JsNumber)) if parseJsNumber(record, name).equalsIgnoreCase("long") =>
          record.put(name, value.value.toLong)
          record
        case (record, (name, value: JsNumber)) if parseJsNumber(record, name).equalsIgnoreCase("float") =>
          record.put(name, value.value.toFloat)
          record
        case (record, (name, value: JsNumber)) if parseJsNumber(record, name).equalsIgnoreCase("double") =>
          record.put(name, value.value.toDouble)
          record
        case (record, (name, value: JsBoolean)) =>
          record.put(name, value.value)
          record
        case (record, (name, value: JsObject)) =>
          record.put(name, parse(value, name, schemaMap))
          record
        case (record, (name, value: JsArray)) =>
          record.put(name, parseArray(value, name, schemaMap))
          record
      }
  }

  private def parseJsNumber(record: GenericData.Record, name: String): String = {
    val types = record.getSchema.getField(name).schema().getTypes
    val atype = types.size() match {
      case 1 => types.get(0)
      case _ => types.get(1)
    }
    atype.getType.toString
  }

  private def parseArray(jsArray: JsArray, fieldName: String, schemaMap: Map[String, Schema]): util.List[_] = jsArray.elements.map {
    case value: JsString => value.value
    case value: JsObject => parse(value, fieldName, schemaMap)
  }.asJava
}
