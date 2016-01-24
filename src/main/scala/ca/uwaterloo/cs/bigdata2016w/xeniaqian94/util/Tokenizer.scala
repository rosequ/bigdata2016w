package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.util

import java.util.StringTokenizer

import scala.collection.JavaConverters._

trait Tokenizer {
  def tokenize(s: String): List[String] = {
    new StringTokenizer(s).asScala.toList
      .map(_.asInstanceOf[String].toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""))
      .filter(_.length != 0)
  }
}