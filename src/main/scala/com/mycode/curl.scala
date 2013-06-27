//simple curl
package com.mycode
import scalaj.http.{HttpOptions, Http}
import scala.util.parsing.json._

object Curl{
  def printList(args: List[_]): Unit = {
    args.foreach(println)
  }
  def concat(strings: List[String]): String = (strings filter {
    _.nonEmpty
  }).mkString(", ")
  def main(args: Array[String]) {
     val result = Http.postData("http://localhost:8080/twitter_mentioned_hash/for_hashtag/_search",
                """{
                    "partial_fields": {
                      "partial1": {
                        "include": "user.id"
                      }
                    },
                    "query": {
                      "bool": {
                        "must": [
                          {
                            "term": {
                              "for_hashtag.entities.hashtags.text": "nba"
                            }
                          }
                        ],
                        "must_not": [],
                        "should": []
                      }
                    },
                    "from": 0,
                    "size": 5000,
                    "sort": [],
                    "facets": {}
                  }
                  """)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.connTimeout(1000))
      .option(HttpOptions.readTimeout(5000))
      .asString

    var userIDs: List[String] = List[String]()
    var json: Option[Any] = JSON.parseFull(result)
    var records:List[Map[String,Any]] = json.get.asInstanceOf[Map[String, Any]]
                                  .get("hits").get.asInstanceOf[Map[String, Any]]
                                  .get("hits").get.asInstanceOf[List[Map[String,Any]]]
    for (record <- records) {
      val userID:Int = record.get("fields").get.asInstanceOf[Map[String,Any]]
                                           .get("partial1").get.asInstanceOf[Map[String,Any]]
                                           .get("user").get.asInstanceOf[Map[String,Double]]
                                           .get("id").get.toInt

      userIDs = userID.toString() ::userIDs
    }
    var userHashtags: List[String] = List[String]()

    val strOfIDs = concat(userIDs)


    val query =  """{
            "partial_fields": {
              "partial1": {
                "include": [
                  "user.id",
                  "entities.hashtags.text"
                ]
              }
            },
            "filter": {
              "bool": {
                "must_not": {
                  "missing": {
                    "field": "entities.hashtags.text",
                    "existence": true,
                    "null_value": true
                  }
                },
                "must": [
                  {
                    "terms": {
                      "for_hashtag.user.id":[ %s ]}
                  }
                ],
                "should": []
              }
            },
            "from": 0,
            "size": 2000,
            "sort": [],
            "facets": {}
          }
                   """.format(strOfIDs)
      println(query )
      val userRes = Http.postData("http://localhost:8080/twitter_mentioned_hash/for_hashtag/_search",
       query)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.connTimeout(1000))
        .option(HttpOptions.readTimeout(50000))
        .asString

      json = JSON.parseFull(userRes)
      records = json.get.asInstanceOf[Map[String, Any]]
        .get("hits").get.asInstanceOf[Map[String, Any]]
        .get("hits").get.asInstanceOf[List[Map[String,Any]]]
      for (record <- records) {
        val tags:List[Map[String,String]] = record.get("fields").get.asInstanceOf[Map[String,Any]]
          .get("partial1").get.asInstanceOf[Map[String,Any]]
          .get("entities").get.asInstanceOf[Map[String,Any]]
          .get("hashtags").get.asInstanceOf[List[Map[String,String]]]

        for (tag <- tags) {
          userHashtags = tag.get("text").get :: userHashtags
        }
      }

    //printList(userHashtags)
    userHashtags = userHashtags.map(_.toLowerCase.trim)
    val counts: Seq[(String, Int)] = {
      userHashtags.groupBy(identity).mapValues(_.size)
    }.toSeq.sortBy(_._2)
    for((word, count) <- counts) println("%s\t%d".format(word, count))
  }
}