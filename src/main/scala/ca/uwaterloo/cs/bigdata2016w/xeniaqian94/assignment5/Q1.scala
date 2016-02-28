package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment5

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
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "shipdate", required = true)
}
class MyPartitioner(numOfPar: Int) extends Partitioner {
  def numPartitions: Int = numOfPar
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(String, String)]
    ((k._1.hashCode() & Integer.MAX_VALUE) % numPartitions)
  }
}

object Q1 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    println("hello world")
    //    BasicConfigurator.configure();

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    //    System.setProperty("hadoop.home.dir", "/");

    val sc = new SparkContext(conf)

    //    val outputDir = new Path(args.output())
    //    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    //TO DO
    val textFile = sc.textFile(args.input()+"/lineitem.tbl")
    val shipdate=args.date()
    val counts = textFile
            .map(line => line.split("""\|""")(10))
            .filter(_.substring(0,shipdate.length())==shipdate)
            .map(date=>(date,1))
            .reduceByKey(_ + _)
            
            
            
    println("Answer="+counts.take(10).foreach(println))
            
            
    

   
    //      .map(bigram => (bigram, 1))
    //      .reduceByKey(_ + _)
    //      .repartitionAndSortWithinPartitions(new MyPartitioner(args.reducers()))
    //      .mapPartitions(iter=>{
    //        var marginal=1
    //        var freq=List[((String,String),Float)]()
    //        while (iter.hasNext){
    //          val x=iter.next;
    //          if (x._1._2.equals("*"))
    //            marginal=x._2 
    //          else
    //            freq=freq.::((x._1,(1.0f*x._2/marginal)))
    //        }
    //        log.info("Freq.length="+freq.length)
    //        freq.toIterator
    //      })
    //      .cache//      .sortByKey()
    //      .saveAsTextFile(args.output())

  }

}