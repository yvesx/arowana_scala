package com.mycode
import com.redis._
import com.mycode.Utilities._
/**
 * Created with IntelliJ IDEA.
 * User: yves
 * Date: 6/29/13
 * Time: 9:32 PM
 * To change this template use File | Settings | File Templates.
 */
class TwitterAffinity (val termKey: String,
                       val elasticHostPort: String,
                       val redisHost: String,
                       val redisPort: Int ) extends CacheableQuery {

  val r: RedisClient = new RedisClient("localhost",6379)
  val userQuery: ElasticQuery = new ElasticQuery(
    host = elasticHostPort,
    index = "twitter_mentioned_hash",
    mapping = "for_hashtag",
    must = "[{\"term\": {\"entities.hashtags.text\": \"%s\"} }]".format(termKey),
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
        .get("id").get.toInt.toString

      userIDs = userID +: userIDs
    }
    //search cache first
    val cache: Seq[(Any, Option[String])] = mgetZip(userIDs,r.mget(Nil,userIDs:_*).get)
    val tags = ZipHits(cache).map(_._2.get)
    val missedUsers = ZipMiss(cache).map(_._1.toString)
    return Map("tagsFromCache"->tags,"missedUsers" -> missedUsers)
    //return com.mycode.Utilities.concat(missedUsers,",")
  }

  def findAffinities: Seq[String] = {
    // try affinity cache first
    r.get("twitterHashtag:%s".format(termKey)) match{
      case Some(s) => return s.split(",").toSeq
      case None =>
    }
    //
    val usersMixed = parseUsers
    println(usersMixed.get("missedUsers").get.size)
    println(usersMixed.get("tagsFromCache").get.size)
    val records = affQuery(concat(usersMixed.get("missedUsers").get,",")).parseElasticHits()
    val tagsFromCache = usersMixed.get("tagsFromCache").get.map(_.split(",")).map(_(0)).asInstanceOf[List[Any]]
    var userHashtags: List[Any] = flatten(tagsFromCache)
    var KeyValuePairs: Seq[(String,String)] = Seq[(String,String)]()

    for (record <- records) {
      val tags:List[String] = record.get("fields").get.asInstanceOf[Map[String,Any]]
        .get("partial1").get.asInstanceOf[Map[String,Any]]
        .get("entities").get.asInstanceOf[Map[String,Any]]
        .get("hashtags").get.asInstanceOf[List[Map[String,String]]].map(_.get("text").get)

      val userID:String = record.get("fields").get.asInstanceOf[Map[String,Any]]
        .get("partial1").get.asInstanceOf[Map[String,Any]]
        .get("user").get.asInstanceOf[Map[String,Any]]
        .get("id").get.asInstanceOf[Double].toInt.toString
      //userIDs = userIDs :+ userID
      val tagsString:String = concat(tags,",")
      KeyValuePairs = (userID,tagsString) +: KeyValuePairs

      userHashtags = tags ++ userHashtags

    }
    //Store to cache, if parseAffinities is called. mset is called every time
    r.mset(KeyValuePairs:_*)
    r.set("twitterHashtag:%s".format(termKey),concat(userHashtags.asInstanceOf[List[String]],","))
    return userHashtags.asInstanceOf[List[String]]
  }

  def mapReduceAffinities: Seq[(String, Int)] =
    findAffinities.map(_.toLowerCase.trim).groupBy(identity).mapValues(_.size).toSeq.filter(_._2>2).sortBy(_._2)

  def printAffinities: Unit = com.mycode.Utilities.printSeq(mapReduceAffinities)
}
