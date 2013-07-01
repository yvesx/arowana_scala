package com.mycode
import com.redis._
/**
 * Created with IntelliJ IDEA.
 * User: yves
 * Date: 6/29/13
 * Time: 9:32 PM
 * To change this template use File | Settings | File Templates.
 */
class TwitterAffinity () extends CacheableQuery {
  val cacheHost: String = "localhost"
  val cachePort: Int = 6379
  val userQuery: ElasticQuery = new ElasticQuery(
    host = "http://localhost:8080",
    index = "twitter_mentioned_hash",
    mapping = "for_hashtag",
    must = "[{\"term\": {\"entities.hashtags.text\": \"nba\"} }]",
    mustNot = "[]",
    partial_fields = "{\"partial1\": { \"include\": \"user.id\"} }",
    size = "1000"
  )
  def affQuery: ElasticQuery = new ElasticQuery(
    host = userQuery.host,
    index = userQuery.index,
    mapping = userQuery.mapping,
    must = """[ { "terms": { "for_hashtag.user.id":[ %s ]} } ]""".format(parseUsers),
    mustNot = """{ "missing": { "field": "entities.hashtags.text", "existence": true, "null_value": true } }""",
    partial_fields = """{"partial1": { "include": [ "user.id", "entities.hashtags.text"]}}""",
    size = userQuery.size
  )
  def parseUsers: String = {
    val records = userQuery.parseElasticHits()
    var userIDs: Vector[String] = Vector[String]()
    for (record <- records) {
      val userID:String = record.get("fields").get.asInstanceOf[Map[String,Any]]
        .get("partial1").get.asInstanceOf[Map[String,Any]]
        .get("user").get.asInstanceOf[Map[String,Double]]
        .get("id").get.toString

      userIDs = userIDs:+ userID
    }
    //search cache first
    val cache: Seq[(Any, Option[String])] = mgetZip(userIDs:_*)
    ZipHits(cache)
    ZipMiss(cache)
    return com.mycode.Utilities.concat(userIDs,",")
  }

  def parseAffinities: Vector[String] = {
    val records = affQuery.parseElasticHits()
    var userHashtags: Vector[String] = Vector[String]()
    var KeyValuePairs: Seq[(String,String)] = Seq[(String,String)]()
    for (record <- records) {
      val tags:List[String] = record.get("fields").get.asInstanceOf[Map[String,Any]]
        .get("partial1").get.asInstanceOf[Map[String,Any]]
        .get("entities").get.asInstanceOf[Map[String,Any]]
        .get("hashtags").get.asInstanceOf[List[Map[String,String]]].map(_.get("text").get)

      val userID:String = record.get("fields").get.asInstanceOf[Map[String,Any]]
        .get("partial1").get.asInstanceOf[Map[String,Any]]
        .get("user").get.asInstanceOf[Map[String,Any]]
        .get("id").get.asInstanceOf[Double].toString
      //userIDs = userIDs :+ userID
      val tagsString:String = com.mycode.Utilities.concat(tags,",")
      KeyValuePairs = KeyValuePairs :+ (userID,tagsString)
      for (tag <- tags) {
        userHashtags = userHashtags :+ tag
      }
    }
    //Store to cache, if parseAffinities is called. mset is called every time
    mset(KeyValuePairs:_*)
    return userHashtags
  }

  def mapReduceAffinities: Seq[(String, Int)] =
    parseAffinities.map(_.toLowerCase.trim).groupBy(identity).mapValues(_.size).toSeq.sortBy(_._2)

  def printAffinities: Unit = com.mycode.Utilities.printSeq(mapReduceAffinities)
}
