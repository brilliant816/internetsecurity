package me.bayee.internetsecurity.pojo

import org.apache.avro.Schema

/**
  * Created by mofan on 16-10-23.
  */
trait BasicPojo {
  val key: String
  val schemaPath: String
  val schema: Schema
  val schemaMap: Map[String, Schema]

  def generateSchema(parser: Schema.Parser): (Schema, Schema.Parser) = (parser.parse(this.getClass.getClassLoader.getResourceAsStream(schemaPath)), parser)

  def buildSchema(basicPojos: BasicPojo*): Schema = basicPojos.foldLeft((null: Schema, new Schema.Parser()))((last, cur) => cur.generateSchema(last._2))._1
}
