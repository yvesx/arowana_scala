package com.mycode

/**
 * Created with IntelliJ IDEA.
 * User: yves
 * Date: 6/29/13
 * Time: 8:26 PM
 * To change this template use File | Settings | File Templates.
 */
class ElasticQuery (
  val host: String,
  val index: String,
  val mapping: String,
  val searchFields: String,  //  "term": {"entities.hashtags.text": "%s"}
  val selectFields: String,  //  "partial1": { "include": "user.id"}
  val size: String
  ) {
   def query: String = """{
                    "partial_fields": { %s },
                    "query": {
                      "bool": {
                        "must": [
                          { %s }
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
                       """.format(selectFields , searchFields , size)
}
