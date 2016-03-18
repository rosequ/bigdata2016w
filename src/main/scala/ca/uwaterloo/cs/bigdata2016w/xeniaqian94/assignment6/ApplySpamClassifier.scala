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
    val weight = modelFile.map(line => (line.split("[,()]")(1).toInt, line.split("[,()]")(2).toDouble)).collectAsMap
    val broadcastWeight=sc.broadcast(weight)
    
    def spamminess(features: Array[Int]): Double = {
      val w=broadcastWeight.value
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }
    
    val testLabel = sc.textFile(args.input()).map(line=>{
      val instanceArray = line.split(" ")
      val docid = instanceArray(0)
      val isSpamlabel = instanceArray(1)
      val features = instanceArray.slice(2, instanceArray.length).map{ featureIndex => featureIndex.toInt }
      val spamScore = spamminess(features)
      
      var isSpamJudge = "spam"
      if (!(spamScore > 0)) {
        isSpamJudge = "ham"
      } 
      (docid, isSpamlabel, spamScore, isSpamJudge)
    })
    .saveAsTextFile(args.output())

  }

}
