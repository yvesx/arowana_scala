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
  def mset(keyValuePairs:(Any,Any)*): Boolean = {
    r.mset(keyValuePairs:_*)
  }
  def getCacheHits(keys:Any*): Double = {
    val lst = mget(keys)
    lst.count(_.isDefined)/lst.size
  }
}
