package me.bayee.internetsecurity.model

import me.bayee.internetsecurity.pojo.ModelInput
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.XML

/**
  * Created by mofan on 16-9-27.
  */
object AttactDetectionModel extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("attact_detection_model.xml"))
    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val input = sc
      .textFile((xml \ "input").text)
      .map(ModelInput.fromHttpTrafficLog)

    // start the model
    val base = input.filter(_.httpCode.getOrElse(-1) == 200)

    (xml \ "rules" \ "rule").foreach { node =>
      base.filter(mi => (node \ "regex").text.r.findFirstIn(mi.url.getOrElse("")).isDefined)
        .map(_.toKeyValueWithId((node \ "id").text))
        .saveAsSequenceFile((node \ "hdfsPath").text)
    }
  }
}
