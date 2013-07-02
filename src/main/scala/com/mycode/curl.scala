//simple curl
/*
* arguments:
* <host_and_path> http://localhost:8080/twitter_mentioned_hash/for_hashtag/_search
* <hashtag_to_search> nba
* <size> 1000*/
package com.mycode

import com.redis._

object Curl{
  def main(args: Array[String]) {
    val ta: TwitterAffinity = new TwitterAffinity(termKey=args(0),elasticHostPort = args(1),
                                                  redisHost =args(2),redisPort = args(3).toInt)
    ta.printAffinities
  }
}