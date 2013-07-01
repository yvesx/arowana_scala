//simple curl
/*
* arguments:
* <host_and_path> http://localhost:8080/twitter_mentioned_hash/for_hashtag/_search
* <hashtag_to_search> nba
* <size> 1000*/
package com.mycode

import com.redis._

object Curl{
  def printList(args: List[_]): Unit = {
    args.foreach(println)
  }
  def concat(strings: List[String]): String = (strings filter {
    _.nonEmpty
  }).mkString(", ")
  def main(args: Array[String]) {

     //Redis stuff
     val r = new RedisClient("localhost", 6379)
     //println(r)
     var affinity_cache_hit = 0
     val host_and_path = args(0)
     val hashtag_to_search = args(1)
     val size = args(2)

     val affinities = r.get("%s:affinities".format(hashtag_to_search))
     if (!affinities.isEmpty){
       affinity_cache_hit = 1
       // print results
       println(affinities.get)
       System.exit(0)
     }
     val result = Http.postData(host_and_path,
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
                              "for_hashtag.entities.hashtags.text": "%s"
                            }
                          }
                        ],
                        "must_not": [],
                        "should": []
                      }
                    },
                    "from": 0,
                    "size": %s,
                    "sort": [],
                    "facets": {}
                  }
                  """.format(hashtag_to_search , size))
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.connTimeout(1000))
      .option(HttpOptions.readTimeout(10000))
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


    val strOfIDs = concat(userIDs)

    var userHashtags: List[String] = List[String]()
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
            "size": %s,
            "sort": [],
            "facets": {}
          }
                   """.format(strOfIDs , size)
      //println(query )
      val userRes = Http.postData(host_and_path,
       query)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.connTimeout(1000))
        .option(HttpOptions.readTimeout(10000))
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
    var affinity_res: String = ""
    for((word, count) <- counts) {
      if (count > 10) {
        affinity_res = "%s%s".format(affinity_res, "%s\t%d\n".format(word, count))
      }
    }
    r.set("%s:affinities".format(hashtag_to_search), affinity_res)
    println(affinity_res)
  }
}