package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment6

import ca.uwaterloo.cs.bigdata2016w.xeniaqian94.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import java.util.StringTokenizer
import scala.collection.JavaConverters._
import scala.math._

object ApplySpamClassifier extends Tokenizer {
  def main(argv: Array[String]) {
    val log = Logger.getLogger(getClass().getName())

    val args = new Conf(argv)
    println("hello world")
    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val modelFile = sc.textFile(args.model())
    val w = modelFile.map(line => (line.split("[,()]")(1).toInt, line.split("[,()]")(2).toDouble)).collectAsMap

    def main(argv: Array[String]) {

      def spamminess(features: Array[Int]): Double = {
        var score = 0d
        features.foreach(f => if (w.contains(f)) score += w(f))
        score
      }
      //TO DO
      val textFile = sc.textFile(args.input());
      val testLabel = textFile.map(line => {
        val instanceArray = line.split(" ")
        val docid = instanceArray(0)
        //      var isSpam = 0
        //      if (instanceArray(1).equals("spam")) {
        //        isSpam = 1
        //      }
        val isSpam = instanceArray(1)
        val features = instanceArray.slice(2, instanceArray.length).map { featureIndex => featureIndex.toInt }
        // Parse input
        // ..
        (docid, isSpam, features)
      }).map(instance => {
        val docid = instance._1
        val isSpamlabel = instance._2
        val spamScore = spamminess(instance._3)
        var isSpamJudge = "spam"
        if (!(spamScore > 0)) {
          isSpamJudge = "ham"
        }
        (docid, isSpamlabel, spamScore, isSpamJudge)
      })
      testLabel.saveAsTextFile(args.output());

    }
  }

}
