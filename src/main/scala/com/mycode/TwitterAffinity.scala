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
  def affQuery(userIDs: String): ElasticQuery = new ElasticQuery(
    host = userQuery.host,
    index = userQuery.index,
    mapping = userQuery.mapping,
    must = """[ { "terms": { "for_hashtag.user.id":[ %s ]} } ]""".format(userIDs),
    mustNot = """{ "missing": { "field": "entities.hashtags.text", "existence": true, "null_value": true } }""",
    partial_fields = """{"partial1": { "include": [ "user.id", "entities.hashtags.text"]}}""",
    size = userQuery.size
  )
  def parseUsers: Map[String,Seq[String]] = {
    val records = userQuery.parseElasticHits()
    var userIDs: List[String] = List[String]()
    for (record <- records) {
      val userID:String = record.get("fields").get.asInstanceOf[Map[String,Any]]
        .get("partial1").get.asInstanceOf[Map[String,Any]]
        .get("user").get.asInstanceOf[Map[String,Double]]
        .get("id").get.toString

      userIDs = userID +: userIDs
    }
    //search cache first
    val cache: Seq[(Any, Option[String])] = mgetZip(userIDs:_*)
    val tags = ZipHits(cache).map(_._2.get)
    val missedUsers = ZipMiss(cache).map(_._1.toString)
    return Map("tagsFromCache"->tags,"missedUsers" -> missedUsers)
    //return com.mycode.Utilities.concat(missedUsers,",")
  }

  def findAffinities: List[String] = {
    // try affinity cache first

    //
    val usersMixed = parseUsers
    val records = affQuery(com.mycode.Utilities.concat(usersMixed.get("missedUsers").get,",")).parseElasticHits()
    var userHashtags: List = com.mycode.Utilities.flatten(usersMixed.get("tagsFromCache").get.map(_.split(",").asInstanceOf[List[String]]).asInstanceOf[List])
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
      KeyValuePairs = (userID,tagsString) +: KeyValuePairs

      userHashtags = tags ++ userHashtags

    }
    //Store to cache, if parseAffinities is called. mset is called every time
    mset(KeyValuePairs:_*)
    return userHashtags.asInstanceOf[List[String]]
  }

  def mapReduceAffinities: Seq[(String, Int)] =
    findAffinities.map(_.toLowerCase.trim).groupBy(identity).mapValues(_.size).toSeq.sortBy(_._2)

  def printAffinities: Unit = com.mycode.Utilities.printSeq(mapReduceAffinities)
}
