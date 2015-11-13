package org.validoc.helloSpark

import scala.io.Source
import scala.io.Source
import java.io.BufferedReader
import scala.io.BufferedSource

object TestingSource {
  def file = "src/main/resources/SmallFile.txt"
  val chunkSize: Int = 100000

  def test(fns: (BufferedSource) => Unit*) = {
    for (i <- 1 to Int.MaxValue) {
      if (i % chunkSize == 0) println(i)
      fns.foreach { fn => fn(Source.fromFile(file)) }
    }
  }
  def main(args: Array[String]): Unit = {
    test(_.getLines, _.getLines().next(), _.iter, _.iter.next)
  }
}