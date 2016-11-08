package me.bayee.internetsecurity.r

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date
import sys.process._
import scala.xml.XML

/**
  * Created by mofan on 16-11-5.
  */
object RPredictJob extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("r_predict_job.xml"))
    val hiveIp = (xml \ "hive" \ "ip").text
    val hivePort = (xml \ "hive" \ "port").text
    val hiveUserName = (xml \ "hive" \ "userName").text
    val hivePassword = (xml \ "hive" \ "password").text
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(sdf.format(new Date))
    println("---- start ----")
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val con = DriverManager.getConnection(s"jdbc:hive2://$hiveIp:$hivePort/default", hiveUserName, hivePassword)
    val stmt = con.createStatement()
    val result = stmt.executeQuery(s"select table_name, param_key from ${(xml \ "r" \ "inputTable").text} group by table_name,param_key")
    while (result.next()) {
      val fileName = (result.getString(1) + "_" + result.getString(2)).replace(".", "_")
      val testFile = s"ssh ${(xml \ "r" \ "remoteServer").text} ls ${(xml \ "r" \ "outputFolder").text}$fileName.RData"
      println(s"test file : $testFile")
      if ((testFile !) == 0) {
        val cmd = s"ssh ${(xml \ "r" \ "remoteServer").text} Rscript ${(xml \ "r" \ "script").text} '$hiveIp' '$hivePort' '$hiveUserName' '$hivePassword' '${(xml \ "r" \ "inputTable").text}' '${result.getString(1)}' '${result.getString(2)}' '${(xml \ "r" \ "outputFolder").text}$fileName'"
        println(s"cmd : $cmd")
        cmd !
      }
    }
    println("---- end ----")
    println(sdf.format(new Date))
  }
}
