package com.mycode

/**
 * Created with IntelliJ IDEA.
 * User: yves
 * Date: 6/29/13
 * Time: 8:52 PM
 * To change this template use File | Settings | File Templates.
 */
object Utilities {
  def printList(args: List[_]): Unit = {
    args.foreach(println)
  }
  def concat(strings: Vector[String],delim: String): String = (strings filter {
    _.nonEmpty
  }).mkString(delim)
}
