package me.bayee.internetsecurity.r

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import sys.process._

/**
  * Created by mofan on 16-11-3.
  */
object RJob extends App {
  override def main(args: Array[String]): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(sdf.format(new Date))
    println("---- start ----")
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val con = DriverManager.getConnection("jdbc:hive2://192.168.0.229:10000/default","huayun","12346")
    val stmt = con.createStatement()
    val result = stmt.executeQuery("select table_name, param_key from r_model_input group by table_name,param_key")
    var i =0
    while(result.next()) {
      i += 1
      println(sdf.format(new Date) + " -- " + i)
      val fileName = (result.getString(1)+"_" + result.getString(2)).replace(".","_")
      val cmd = """ssh userproc Rscript /home/mofan/train.R '192.168.0.229' '10000' 'huayun' '12346' 'r_model_input' '""" + result.getString(1) + "' '" + result.getString(2) + """' '200' '0.01' '/home/mofan/tmp/""" + fileName + """'"""
      println(cmd)
      cmd !
    }
    println("---- end ----")
    println(sdf.format(new Date))
  }
}

object WE extends App {
  println("""ssh userproc ls /home/mofan/ """ !)
}