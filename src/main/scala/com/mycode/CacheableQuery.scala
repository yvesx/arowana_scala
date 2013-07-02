package com.mycode
import com.redis._
/**
 * Created with IntelliJ IDEA.
 * User: yves
 * Date: 6/30/13
 * Time: 4:26 PM
 * To change this template use File | Settings | File Templates.
 */
trait CacheableQuery {
   val r:RedisClient

  def mgetZip(keys:List[String],mgetResults:List[Option[String]]): Seq[(Any,Option[String])] = {
    return keys zip mgetResults
  }
  def mgetHits(mgetResults:List[Option[String]]): List[String] = {
    mgetResults.filter(_.isDefined).map(_.get)
  }
  def mgetMiss(mgetResults:List[Option[String]]): List[String] = {
    mgetResults.filterNot(_.isDefined).map(_.get)
  }
  def ZipHits(cache: Seq[(Any,Option[String])]): Seq[(Any,Option[String])] = {
    cache.filter(_._2.isDefined)
  }
  def ZipMiss(cache: Seq[(Any,Option[String])]): Seq[(Any,Option[String])] = {
    cache.filterNot(_._2.isDefined)
  }

  def getCacheHits(keys:Any*): Double = {
    val lst = r.mget(Nil,keys:_*).get
    lst.count(_.isDefined)/lst.size
  }
}
