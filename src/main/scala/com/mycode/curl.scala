//simple curl
/*
* arguments:
* <fb_id/term> 1001/nba
* <host_and_path> http://localhost:8080/
* <redisHost> localhost
* <redisPort> ??
* <src> fb*/
package com.mycode

import com.redis._

object Curl{
  def main(args: Array[String]) {
    if (args(4) == "fb"){
      val fa: FacebookAffinity = new FacebookAffinity(fb_id=args(0),elasticHostPort = args(1),
        redisHost =args(2),redisPort = args(3).toInt)
      println(fa.printAffinities )
    }
    else{
    val ta: TwitterAffinity = new TwitterAffinity(termKey=args(0),elasticHostPort = args(1),
                                                  redisHost =args(2),redisPort = args(3).toInt)
    ta.printAffinities
    }
  }
}