package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment2


import ca.uwaterloo.cs.bigdata2016w.xeniaqian94.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner


class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
}
class MyPartitioner(numOfPar: Int) extends Partitioner {
  def numPartitions: Int = numOfPar
  def getPartition(key:Any): Int = {
    val k=key.asInstanceOf[(String,String)]
    ((k._2.hashCode() & Integer.MAX_VALUE) % numPartitions)
  }
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    //TO DO
    val textFile = sc.textFile(args.input(),args.reducers())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).flatMap(p => List((p(0),p(1)),(p(0),"*"))).toList else List()
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .repartitionAndSortWithinPartitions(new MyPartitioner(args.reducers()))
//      .mapPartitions(iter=>{
//        val margin=new HashMap[String,Int]
//        while (iter.hasNext)
//        
//      })
//      .groupBy{x=>x._1._1}
//      .flatMap(i=>{
//        val margin=i._2.maxBy{x=>x._2}
//        i._2.map(x=>(x._1,(1.0*x._2)/margin._2))       
//      })
//      .filter(i=>(i._1._2!="*"))
    
    counts.saveAsTextFile(args.output())
  }
}
