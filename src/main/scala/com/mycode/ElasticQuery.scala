package com.mycode
import scalaj.http.{HttpOptions, Http}
import scala.util.parsing.json._
/**
 * Created with IntelliJ IDEA.
 * User: yves
 * Date: 6/29/13
 * Time: 8:26 PM
 * To change this template use File | Settings | File Templates.
 */
class ElasticQuery (
  val host: String,          //  "http://localhost:8080"
  val index: String,         //  "twitter_mentioned_hash"
  val mapping: String,       //  "for_hashtag"
  val must: String,  //  "[{\"term\": {\"entities.hashtags.text\": \"nba\"} }]"
  val mustNot: String,
  val partial_fields: String,  //  "{\"partial1\": { \"include\": \"user.id\"} }"
  val size: String           // "1000"
  ) {
   def endPoint: String = "%s/%s/%s/_search".format(host , index , mapping)
   def baseQuery: String =
     """{
            "partial_fields": %s,
            "filter": {
              "bool": {
                "must": %s,
                "must_not": %s,
                "should": []
              }
            },
            "from": 0,
            "size": %s,
            "sort": [],
            "facets": {}
          }
     """.format(partial_fields , must , mustNot , size)

   def retrieveJSON(): String = {
     Http.postData(endPoint, baseQuery)
       .header("Content-Type", "application/json")
       .header("Charset", "UTF-8")
       .option(HttpOptions.connTimeout(4000))
       .option(HttpOptions.readTimeout(40000))
       .asString
   }
   def parseElasticHits(): List[Map[String,Any]] = {
     val jsonString: String = retrieveJSON()
     //println(jsonString)
     val records: List[Map[String,Any]] = JSON.parseFull(jsonString).get.asInstanceOf[Map[String, Any]]
                                        .get("hits").get.asInstanceOf[Map[String, Any]]
                                        .get("hits").get.asInstanceOf[List[Map[String,Any]]]

     return records
   }

}
