package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment6

import ca.uwaterloo.cs.bigdata2016w.xeniaqian94.util.Tokenizer
import Math._

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import java.util.StringTokenizer
import scala.collection.JavaConverters._
import scala.math._

object ApplyEnsembleSpamClassifier extends Tokenizer {
  
  def main(argv: Array[String]) {
    val log = Logger.getLogger(getClass().getName())

    val args = new Conf(argv)
    println("hello world")
    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log info("Method: "+args.method())
    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val modelGroupX = sc.textFile(args.model()+"/part-00000")
    val weightGroupX = modelGroupX.map(line => (line.split("[,()]")(1).toInt, line.split("[,()]")(2).toDouble)).collectAsMap
    val broadcastGroupX=sc.broadcast(weightGroupX)
    
    val modelGroupY = sc.textFile(args.model()+"/part-00001")
    val weightGroupY = modelGroupY.map(line => (line.split("[,()]")(1).toInt, line.split("[,()]")(2).toDouble)).collectAsMap
    val broadcastGroupY=sc.broadcast(weightGroupY)
    
    val modelBritney = sc.textFile(args.model()+"/part-00002")
    val weightBritney=modelBritney.map(line => (line.split("[,()]")(1).toInt, line.split("[,()]")(2).toDouble)).collectAsMap
    val broadcastBritney=sc.broadcast(weightBritney)
    
    def spamminess(features: Array[Int]): (Double,Double,Double) = {
      val wX=broadcastGroupX.value
      val wY=broadcastGroupY.value
      val wBritney=broadcastBritney.value
      var scoreX = 0d
      var scoreY = 0d
      var scoreBritney = 0d
      features.foreach(f => {
        if (wX.contains(f)) scoreX += wX(f)
        if (wY.contains(f)) scoreY += wY(f)
        if (wBritney.contains(f)) scoreBritney += wBritney(f)
      })
      (scoreX,scoreY,scoreBritney)
    }
    
    val spamminessX = (features: Array[Int]) => {
      val wX=broadcastGroupX.value
//      val wY=broadcastGroupY.value
//      val wBritney=broadcastBritney.value
      var scoreX = 0d
//      var scoreY = 0d
//      var scoreBritney = 0d
      features.foreach(f => {
        if (wX.contains(f)) scoreX += wX(f)
//        if (wY.contains(f)) scoreY += wY(f)
//        if (wBritney.contains(f)) scoreBritney += wBritney(f)
      })
//      (scoreX,scoreY,scoreBritney)
      scoreX
    }
    
    val testLabel = sc.textFile(args.input()).map(line=>{
      val instanceArray = line.split(" ")
      val docid = instanceArray(0)
      val isSpamlabel = instanceArray(1)
      val features = instanceArray.slice(2, instanceArray.length).map{ featureIndex => featureIndex.toInt }
//      val score = spamminessX(features)
      val scoreArray = Array(spamminessX(features),spamminessX(features),spamminessX(features))
      var spamScoreString=""
      var spamScore=0d
      if (args.method().equals("average")){
        spamScore=((scoreArray(0)+scoreArray(1)+scoreArray(2))/scoreArray.size)
        spamScoreString=spamScore.toString()
      }else if (args.method().equals("vote")){
        val sigArray=for {score<-scoreArray} yield Math.signum(score).toInt
        spamScore=sigArray(0)+sigArray(1)+sigArray(2)
        spamScoreString=spamScore.toString()
      }
      var isSpamJudge = "spam"
      if (!(spamScore > 0)) {
        isSpamJudge = "ham"
      } 
      (docid, isSpamlabel, spamScore, isSpamJudge)
    })
    .saveAsTextFile(args.output())

  }

}