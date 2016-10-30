package me.bayee.internetsecurity.util

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Created by mofan on 16-10-30.
  */
object RetryUtil {
  val INF = Int.MinValue

  @tailrec
  def retry[T](n: Int)(fn: => T)(implicit logger: (String, Throwable) => Unit = { (s: String, t: Throwable) => Unit }, waitTime: Duration = 1 seconds): T = Try {
    fn
  } match {
    case Success(x) => x
    case Failure(e) if n == INF =>
      logger("retry times: INF", e)
      Thread.sleep(waitTime.toMillis)
      retry(n)(fn)(logger, waitTime)
    case Failure(e) if n > 1 =>
      logger(s"retry times: $n", e)
      Thread.sleep(waitTime.toMillis)
      retry(n - 1)(fn)(logger, waitTime)
    case Failure(e) =>
      logger(s"retry failed.", e)
      throw e
  }
}
