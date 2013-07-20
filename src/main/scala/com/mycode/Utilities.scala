package com.mycode

/**
 * Created with IntelliJ IDEA.
 * User: yves
 * Date: 6/29/13
 * Time: 8:52 PM
 * To change this template use File | Settings | File Templates.
 */
object Utilities {
  def printSeq(args: Seq[_]): Unit = {
    args.foreach(println)
  }
  def mapReduceAffinities(findAffinities: Seq[String]): Seq[(String, Int)] = {
    findAffinities.map(_.toLowerCase.trim).groupBy(identity).mapValues(_.size).toSeq.filter(_._2>2).sortBy(_._2)
  }
  def concat(strings: Seq[String],delim: String): String = (strings filter {
    _.nonEmpty
  }).mkString(delim)
  def flatten[T](list: List[T]): List[T] = list match {
    case Nil => Nil
    case head :: tail => (head match {
      case l: List[T] => flatten(l)
      case i => List(i)
    }) ::: flatten(tail)
  }
}
