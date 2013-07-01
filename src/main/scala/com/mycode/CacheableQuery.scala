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
   var cacheHost: String
   var cachePort: Int
   val r:RedisClient = new RedisClient(cacheHost, cachePort)
   def mget(keys:Any*): List[Option[String]] = {
     r.mget(Nil,keys:_*).get
   }
  def mgetZip(keys:Any*): Seq[(Any,Option[String])] = {
    return keys zip mget(keys)
  }
  def mgetHits(keys:Any*): List[String] = {
    mget(keys).filter(_.isDefined).map(_.get)
  }
  def mgetMiss(keys:Any*): List[String] = {
    mget(keys).filterNot(_.isDefined).map(_.get)
  }
  def ZipHits(cache: Seq[(Any,Option[String])]): Seq[(Any,Option[String])] = {
    cache.filter(_._2.isDefined)
  }
  def ZipMiss(cache: Seq[(Any,Option[String])]): Seq[(Any,Option[String])] = {
    cache.filterNot(_._2.isDefined)
  }
  def mset(keyValuePairs:(Any,Any)*): Boolean = {
    r.mset(keyValuePairs:_*)
  }
  def getCacheHits(keys:Any*): Double = {
    val lst = r.mget(Nil,keys:_*).get
    lst.count(_.isDefined)/lst.size
  }
}
