package me.bayee.internetsecurity.flow

import java.sql.DriverManager

import me.bayee.internetsecurity.pojo.ModelInput
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.XML

/**
  * Created by mofan on 16-10-16.
  */
object MergeJob extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("merge_job.xml"))
    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val pipe = (xml \ "inputs" \ "input").foldLeft[(RDD[(String, (ModelInput, List[String]))])](null) { (rdd, node) =>
      val pipe = sc.sequenceFile[String, String]((node \ "hdfsPath").text).map(kv => (kv._1, ModelInput.fromModelInput(kv._2)))
      if (rdd == null) pipe.map(kv => (kv._1, (kv._2._1, List(kv._2._2))))
      else rdd.fullOuterJoin(pipe).map {
        case (key, (Some(v1), Some(v2))) => (key, (v1._1, v2._2 :: v1._2))
        case (key, (None, Some(v2))) => (key, (v2._1, List(v2._2)))
        case (key, (Some(v1), None)) => (key, v1)
      }
    }

        pipe
          .foreachPartition { iter =>
            val conf = HBaseConfiguration.create()
            conf.set("hbase.zookeeper.property.clientPort", "2181")
            conf.set("hbase.zookeeper.quorum", "192.168.0.229")
            conf.set("hbase.master", "192.168.0.229:600000")

            val table = new HTable(conf, "internet_security")
            iter.foreach {
              case (key, (mi, ids)) =>
                val put = new Put(s"${mi.visitTime.get}#${mi.serverIp.getOrElse("")}".getBytes)
                put.addColumn("cf".getBytes, "visitTime".getBytes, mi.visitTime.getOrElse("").getBytes)
                put.addColumn("cf".getBytes, "clientIp".getBytes, mi.clientIp.getOrElse("").getBytes)
                put.addColumn("cf".getBytes, "serverIp".getBytes, mi.serverIp.getOrElse("").getBytes)
                put.addColumn("cf".getBytes, "url".getBytes, mi.url.getOrElse("").getBytes)
                put.addColumn("cf".getBytes, "httpCode".getBytes, mi.httpCode.getOrElse(-1).toString.getBytes)
                put.addColumn("cf".getBytes, "referInfo".getBytes, mi.referInfo.getOrElse("").getBytes)
                put.addColumn("cf".getBytes, "httpMethod".getBytes, mi.httpMethod.getOrElse("").getBytes)
                put.addColumn("cf".getBytes, "userAgent".getBytes, mi.userAgent.getOrElse("").getBytes)
                put.addColumn("cf".getBytes, "cookie".getBytes, mi.cookie.getOrElse("").getBytes)
                put.addColumn("cf".getBytes, "ids".getBytes, ids.mkString(",").getBytes)
                table.put(put)
            }
            table.close
          }

    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val connection = DriverManager.getConnection("jdbc:mysql://192.168.0.226/test", "root", "pass0rd")
    pipe
      .map {
        case (key, (mi, ids)) => ((mi.visitTime.getOrElse("").split(" ")(0), ids.mkString(","), mi.url.getOrElse(""), 1), 1)
      }
      .reduceByKey(_ + _)
      .take(Int.MaxValue)
      .foreach {
        case ((date, threatType, url, threatLevel), threatCount) =>
          val statement = connection.prepareStatement("insert into threat_log_summary (threat_date, threat_type, url, threat_level, threat_count) values (?,?,?,?,?)")
          statement.setString(1, date)
          statement.setString(2, threatType)
          statement.setString(3, url)
          statement.setInt(4, threatLevel)
          statement.setInt(5, threatCount)
          statement.execute
          statement.close()
      }
    connection.close()
  }
}