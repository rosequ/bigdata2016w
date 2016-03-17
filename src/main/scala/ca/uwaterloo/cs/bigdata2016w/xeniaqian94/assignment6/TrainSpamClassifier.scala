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

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = false)
  val output = opt[String](descr = "output path", required = false)
  
}
//class MyPartitioner(numOfPar: Int) extends Partitioner {
//  
////  def numPartitions: Int = numOfPar
////  def getPartition(key: Any): Int = {
////    val k = key.asInstanceOf[(String, String)]
////    ((k._1.hashCode() & Integer.MAX_VALUE) % numPartitions)
////  }
//}

object TrainSpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)
    println("hello world")
    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    //    System.setProperty("hadoop.home.dir", "/");
    //    val outputDir = new Path(args.output())
    //    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    //TO DO
    val textFile = sc.textFile(args.input());

    val trained = textFile.map(line => {
      val instanceArray = line.split(" ")
      val docid = instanceArray(0)
      var isSpam = 0
      if (instanceArray(1).equals("spam")) {
        isSpam = 1
      }
      val features = instanceArray.slice(2, instanceArray.length).map { featureIndex => featureIndex.toInt }
      // Parse input
      // ..
      (0, (docid, isSpam, features))
    }).groupByKey(1)
    .persist()
    
    

    // w is the weight vector (make sure the variable is within scope) size=1000091 
    var w = Map[Int, Double]()
    var old_w=Map[Int,Double]()
    
    // This is the main learner:
    val delta = 0.002
    var converged = false
    var i = 1
    val numIterations = 10000
    val threshold=1E-8

    def spamminess(features: Array[Int]): Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }
    def isConverged(previousWeights: Map[Int, Double],
      currentWeights: Map[Int, Double],
      threshold: Double): Boolean = {
      // To compare with convergence tolerance.
      var flag=true
      previousWeights.foreach(pair=>{
        if (currentWeights.contains(pair._1)&&(currentWeights(pair._1)-pair._2)>threshold)
          flag=false
      })
      flag
  }


    while (!converged && i < numIterations) {
      //      var currentWeights=trained.context.broadcast(w)
      old_w=w
      trained.foreach(instanceIterable => {
        instanceIterable._2.foreach(tuple => {
          val isSpam = tuple._2
          val features = tuple._3
          val score = spamminess(features)
          val prob = 1.0 / (1 + exp(-score))
          features.foreach(f => {
            if (w.contains(f)) {
              w=w updated (f, w(f) + (isSpam - prob) * delta)
              //        w(f) = w(f)+(isSpam - prob) * delta
            } else {
              w=w updated (f, (isSpam - prob) * delta)
              //        w(f) = (isSpam - prob) * delta
            }
          })

        })
      })
      converged=isConverged(old_w,w,threshold)
      i += 1
    }
    
    // Scores a document based on its list of features.
    val model=sc.parallelize(w.toSeq,1)
    println("finished training in "+i+" iterations, this model has "+model.count().toString())
    model.saveAsTextFile(args.model());

  }

}
