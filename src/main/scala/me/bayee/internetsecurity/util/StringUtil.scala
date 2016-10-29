package me.bayee.internetsecurity.util

import java.net.URLDecoder

import sun.misc.BASE64Decoder

/**
  * Created by mofan on 16-10-27.
  */
object StringUtil {
  implicit def buildRichString(str: String) = RichString(str)
}

case class RichString(str: String) {
  def getUrl: String = {
    def min(ii: Int*) = ii.foldLeft(-1) { (last, cur) =>
      if (cur < 0) last
      else if (last < 0) cur
      else if (last < cur) last
      else cur
    }
    val index0 = str.indexOf("://")
    val _str = if (index0 > 0) str.substring(index0 + "://".length) else str
    val index1 = _str.indexOf("?")
    val index2 = _str.indexOf("/")
    val index3 = _str.indexOf(":")
    val indexMin = min(index1, index2, index3)
    if (indexMin > 0) _str.substring(0, indexMin) else _str
  }

  def getParamString = {
    val index = str.indexOf("?")
    if(index > 0) str.substring(index + 1)
    else ""
  }

  def getParams: List[(String, String)] = {
    val index = str.indexOf("?")
    if (index > 0) str.substring(index + 1).split("&").map { kv =>
      val s = kv.split("=")
      (s(0), s(1))
    }.toList
    else List.empty[(String, String)]
  }

  def postParams: List[(String, String)] =
    if (str.isEmpty) List.empty[(String, String)]
    else str.split("&").map { kv =>
      val s = kv.split("=")
      if (s.size == 1) ("", kv) else (s(0), s(1))
    }.toList

  def base64Decode: String =
    if (str.startsWith("[BASE64-DATA]")) {
      val decoder = new BASE64Decoder
      new String(decoder.decodeBuffer(str.replace("[BASE64-DATA]", "").replace("[/BASE64-DATA]", "")))
    } else str

  def urlDecode: String = URLDecoder.decode(str, "utf-8")
}

object Test extends App {

  import StringUtil._

}