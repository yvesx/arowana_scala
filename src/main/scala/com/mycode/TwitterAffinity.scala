package com.mycode

/**
 * Created with IntelliJ IDEA.
 * User: yves
 * Date: 6/29/13
 * Time: 9:32 PM
 * To change this template use File | Settings | File Templates.
 */
class TwitterAffinity (val userQuery: ElasticQuery) {
  def parseRecordsStringTW(): String = {
    val records = query.parseElasticHits()
    var userIDs: Vector[String] = Vector[String]()
    for (record <- records) {
      val userID:String = record.get("fields").get.asInstanceOf[Map[String,Any]]
        .get("partial1").get.asInstanceOf[Map[String,Any]]
        .get("user").get.asInstanceOf[Map[String,Double]]
        .get("id").get.toString

      userIDs = userIDs:+ userID
    }
    return com.mycode.Utilities.concat(userIDs,",")
  }

  def constructAffinityQuery(): String = {
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
  }
}
