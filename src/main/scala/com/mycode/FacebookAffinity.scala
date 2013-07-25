package com.mycode
import com.redis._
import com.mycode.Utilities._

/**
 * Created with IntelliJ IDEA.
 * User: yves
 * Date: 7/20/13
 * Time: 4:33 AM
 * To change this template use File | Settings | File Templates.
 */
class FacebookAffinity (val fb_id: String,
                        val elasticHostPort: String,
                        val redisHost: String,
                        val redisPort: Int ) extends CacheableQuery {

  val r: RedisClient = new RedisClient(redisHost,redisPort)
  val userQuery: ElasticQuery = new ElasticQuery(
    host = elasticHostPort,
    index = "facebook_graph",
    mapping = "bipartite",
    must = "[{\"term\": {\"fb_id\": \"%s\"} }]".format(fb_id),
    mustNot = "[]",
    partial_fields = "{\"partial1\": { \"include\": \"user_id\"} }",
    size = "2000"
  )
  def affQuery(userIDs: String): ElasticQuery = new ElasticQuery(
    host = userQuery.host,
    index = userQuery.index,
    mapping = userQuery.mapping,
    must = """[ { "terms": { "bipartite.user_id":[ %s ]} } ]""".format(userIDs),
    mustNot = """{ "terms": { "bipartite.fb_id":[ %s ]} }""".format(fb_id),
    partial_fields = """{"partial1": { "include": [ "fb_id"]}}""",
    size = userQuery.size
  )
  def parseUsers: List[String] = {
    val records = userQuery.parseElasticHits()
    var userIDs: List[String] = List[String]()
    for (record <- records) {
      val userID:String = record.get("fields").get.asInstanceOf[Map[String,Any]]
        .get("partial1").get.asInstanceOf[Map[String,Any]]
        .get("user_id").get.asInstanceOf[String]

      userIDs = userID +: userIDs
    }
    return userIDs
  }

  def findAffinities: Seq[String] = {
    // try affinity cache first
    r.get("facebookAff:%s".format(fb_id)) match{
      case Some(s) => return s.split(",").toSeq
      case None =>
    }
    //
    val userIDs = parseUsers

    val records = affQuery(concat(userIDs,",")).parseElasticHits()
    var fb_ids: List[String] = List[String]()

    for (record <- records) {
      val cur_fb_id:String = record.get("fields").get.asInstanceOf[Map[String,Any]]
        .get("partial1").get.asInstanceOf[Map[String,Any]]
        .get("fb_id").get.asInstanceOf[String]
      fb_ids = cur_fb_id +: fb_ids

    }
    //Store to cache, if parseAffinities is called. mset is called every time
    r.set("facebookAff:%s".format(fb_id),concat(fb_ids,","))
    return fb_ids
  }

  def printAffinities: String = Utilities.groupToJSON(Utilities.mapReduceAffinities(findAffinities))
}
