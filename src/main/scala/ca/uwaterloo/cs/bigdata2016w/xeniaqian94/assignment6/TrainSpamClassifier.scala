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

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = false)
}
class MyPartitioner(numOfPar: Int) extends Partitioner {
  
//  def numPartitions: Int = numOfPar
//  def getPartition(key: Any): Int = {
//    val k = key.asInstanceOf[(String, String)]
//    ((k._1.hashCode() & Integer.MAX_VALUE) % numPartitions)
//  }
}

object TrainSpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)
    println("hello world")
    log.info("Input: " + args.input())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    //    System.setProperty("hadoop.home.dir", "/");
    val sc = new SparkContext(conf)
    //    val outputDir = new Path(args.output())
    //    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    //TO DO
    val textFile = sc.textFile(args.input());

    val trained = textFile.map(line =>{
      val instanceArray=line.split(" ").toArray
      val docid=instanceArray(0)
      val isSpam=instanceArray(1)
      val features=instanceArray.slice(2,instanceArray.length)
      // Parse input
    // ..
      (0, (docid, isSpam, features))
  }).groupByKey(1)
  // Then run the trainer...

  trained.saveAsTextFile(args.model());


//// w is the weight vector (make sure the variable is within scope)
//val w = Map[Int, Double]()
//
//// Scores a document based on its list of features.
//def spamminess(features: Array[Int]) : Double = {
//  var score = 0d
//  features.foreach(f => if (w.contains(f)) score += w(f))
//  score
//}
//
//// This is the main learner:
//val delta = 0.002
//
//// For each instance...
//val isSpam = ...   // label
//val features = ... // feature vector of the training instance
//
//// Update the weights as follows:
//val score = spamminess(features)
//val prob = 1.0 / (1 + exp(-score))
//features.foreach(f => {
//  if (w.contains(f)) {
//    w(f) += (isSpam - prob) * delta
//  } else {
//    w(f) = (isSpam - prob) * delta
//   }
//})
//
//    val shipdate=args.date()
//    val counts = textFile
//            .map(line => line.split("""\|""")(10))
//            .filter(_.substring(0,shipdate.length())==shipdate)
//            .map(date=>("count",1))
//            .reduceByKey(_ + _)
//       
//    println("ANSWER="+counts.lookup("count")(0))

  }

}
