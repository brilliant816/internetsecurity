package me.bayee.internetsecurity.model

import me.bayee.internetsecurity.pojo.ModelInput
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.XML

/**
  * Created by mofan on 16-9-18.
  */
object GraphDetectionModel extends App {

  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("graph_detection_model.xml"))
    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs","false")
    val sc = new SparkContext(conf)
    //    // read the source
    val input = sc
      .textFile((xml \ "input").text)
      .map(ModelInput.fromHttpTrafficLog)

        // start the model
        val inDegreeZero = input.filter(_.referInfo.isEmpty)
        val filterUrl = inDegreeZero
          .map(mi => (mi.url.getOrElse(""), mi.clientIp.getOrElse("")))
          .distinct
          .map(x => (x._1, 1))
          .reduceByKey(_ + _)
          .filter(_._2 <= 10)

        val base = inDegreeZero
          .map(mi => (mi.url.getOrElse(""), mi))
          .leftOuterJoin(filterUrl)
          .filter(_._2._2.isDefined)
          .map(_._2._1)

        (xml \ "rules" \ "rule").foreach { node =>
          base.filter(mi => (node \ "regex").text.r.findFirstIn(mi.url.getOrElse("")).isDefined)
            .map(_.toKeyValueWithId((node \ "id").text))
            .saveAsSequenceFile((node \ "hdfsPath").text)
        }
  }
}