package me.bayee.internetsecurity.flow

import java.sql.DriverManager
import java.util

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

    val idWeightMap = (xml \ "group" \ "record").map(node => ((node \ "id").text, (node \ "weight").text.toInt)).foldLeft(new util.HashMap[String, Int]()) { (last, cur) =>
      last.put(cur._1, cur._2)
      last
    }

    val id2Props = (xml \ "names" \ "record").map(node => ((node \ "id").text, ((node \ "name").text, (node \ "weight").text.toInt, (node \ "threatLevel").text.toInt))).foldLeft(new util.HashMap[String, (String, Int, Int)]()) { (last, cur) =>
      last.put(cur._1, cur._2)
      last
    }

    val pipe = (xml \ "inputs" \ "input").foldLeft[(RDD[(String, (ModelInput, List[String]))])](null) { (rdd, node) =>
      val pipe = sc.sequenceFile[String, String]((node \ "hdfsPath").text).map(kv => (kv._1, ModelInput.fromModelInput(kv._2)))
      if (rdd == null) pipe.map(kv => (kv._1, (kv._2._1, List(kv._2._2))))
      else rdd.fullOuterJoin(pipe).map {
        case (key, (Some(v1), Some(v2))) => (key, (v1._1, v2._2 :: v1._2))
        case (key, (None, Some(v2))) => (key, (v2._1, List(v2._2)))
        case (key, (Some(v1), None)) => (key, v1)
      }
    }
      //爬虫检测中按照权重叠加，超过阀值报警
      .map {
      case (key, (mi, ids)) =>
        val idNotInGroup = ids.foldLeft(false) { (last, cur) =>
          if (last) last
          else !idWeightMap.keySet.contains(cur)
        }
        if (idNotInGroup) (key, (mi, ids, ids))
        else {
          val totalWeight = ids.foldLeft(0) { (last, cur) => last + idWeightMap.get(cur) }
          if (totalWeight >= (xml \ "group" \ "lowBound").text.toInt) (key, (mi, ids, ids))
          else (key, (mi, ids, List.empty[String]))
        }
    }
      .filter(_._2._3.nonEmpty)

    pipe
      .map {
        case (key, (mi, ids, _)) =>
          (mi, ids.map(id => id2Props.get(id)._1))
      }
      .foreachPartition { iter =>
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", (xml \ "hbase" \ "zookeeper" \ "property" \ "clientPort").text)
        conf.set("hbase.zookeeper.quorum", (xml \ "hbase" \ "zookeeper" \ "quorum").text)
        conf.set("hbase.master", (xml \ "hbase" \ "master").text)

        val table = new HTable(conf, "Bdsec_ns1:internet_security")
        iter.foreach {
          case (mi, ids) =>
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
    val connection = DriverManager.getConnection((xml \ "mysql" \ "host").text, (xml \ "mysql" \ "username").text, (xml \ "mysql" \ "password").text)
    pipe
      .map {
        case (key, (mi, _, targetIds)) =>
          val targetId = targetIds.map(id2Props.get).sortWith(_._2 > _._2).head
          ((mi.visitTime.getOrElse("").split(" ")(0), targetId._1, mi.url.getOrElse(""), targetId._3), 1)
      }
      .reduceByKey(_ + _)
      .toLocalIterator
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