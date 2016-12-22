package me.bayee.internetsecurity.hbase

import me.bayee.internetsecurity.pojo.{HttpTrafficLog}
import me.bayee.internetsecurity.util.AvroUtil._
import me.bayee.internetsecurity.util.StringUtil._
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._
import org.apache.hadoop.conf.Configuration 
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result 
import org.apache.hadoop.hbase.io.ImmutableBytesWritable 
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import scala.xml.XML
import org.apache.avro.Schema
/**
  * Created by mofan on 16-10-24.
  */
object HbaseData extends App {
  override def main(args: Array[String]): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream("hbase_data.xml"))
    val dir = args(0) // /2016/11/11

    val hbaseTableName =  (xml \ "tableName" ).text
    val hdfsPath =  (xml \ "outputDir").text +"/"+ dir

    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
     
    val hbaseConf = HBaseConfiguration.create()
    //conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    
    //设置查询的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
    
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    
      
    val job = new Job(sc.hadoopConfiguration)
    val avroSchema = new Schema.Parser().parse(this.getClass.getClassLoader.getResourceAsStream("schema/HttpTrafficLog.avsc"))
    AvroJob.setOutputKeySchema(job,avroSchema)
    //遍历输出
    hbaseRDD.filter{case(key,result) => {
      new String(result.getValue("TRAFFIC_CF".getBytes,"log_time".getBytes)).replace("-","/").contains(dir)
      
    }}.map{
      case (_,result) =>
        val visitTime = Some(new String(result.getValue("TRAFFIC_CF".getBytes,"log_time".getBytes)))
         val httpCode = Some(new String(result.getValue("TRAFFIC_CF".getBytes,"res_code".getBytes)).toInt)
        val serverIp = Some(new String(result.getValue("TRAFFIC_CF".getBytes,"dst_ip".getBytes)))
        val serverPort = Some(new String(result.getValue("TRAFFIC_CF".getBytes,"dst_port".getBytes)).toInt)
        val clientRequestSize =Some(new String(result.getValue("TRAFFIC_CF".getBytes,"req_pack_content".getBytes)).length.toLong)
        val serverResponseSize = Some(new String(result.getValue("TRAFFIC_CF".getBytes,"rep_pack_content".getBytes)).length.toLong)
        val cookie = Some(new String(result.getValue("TRAFFIC_CF".getBytes,"cookie".getBytes)))
        val clientIp = Some(new String(result.getValue("TRAFFIC_CF".getBytes,"source_ip".getBytes)))
        val userAgent = Some(new String(result.getValue("TRAFFIC_CF".getBytes,"useragent".getBytes)))
        val uri = Some(new String(result.getValue("TRAFFIC_CF".getBytes,"req_url".getBytes)))
        val queryParam = Some(new String(result.getValue("TRAFFIC_CF".getBytes,"query_string".getBytes)))
        val httpMethod = Some(new String(result.getValue("TRAFFIC_CF".getBytes,"http_method".getBytes)))
        new HttpTrafficLog(visitTime,Some(""),httpCode,Some(""),serverIp,Some(""),serverPort,clientRequestSize,serverResponseSize,
            Some(""),cookie,clientIp,userAgent,uri,queryParam,httpMethod,Some(""),Some(""))
    }.map(
        log => 
          (
              (new AvroKey(log.toJson.asJsObject.convert2GenericRecord(HttpTrafficLog.key, HttpTrafficLog.schemaMap)), null)
          )
     )
    .saveAsNewAPIHadoopFile(hdfsPath, classOf[GenericRecord], classOf[NullWritable], classOf[AvroKeyOutputFormat[GenericRecord]], job.getConfiguration)
  }
}