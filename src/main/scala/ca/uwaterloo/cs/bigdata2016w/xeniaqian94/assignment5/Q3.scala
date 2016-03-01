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

object Q3 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    println("hello world")
    //    BasicConfigurator.configure();

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q3")
    //    System.setProperty("hadoop.home.dir", "/");

    val sc = new SparkContext(conf)

    //    val outputDir = new Path(args.output())
    //    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    //TO DO
    //    val lineitemTextFile = sc.textFile(args.input() + "/lineitem.tbl")
    //    val ordersTextFile = sc.textFile(args.input() + "/orders.tbl")
    val shipdate = args.date()

    //    val counts = ordersTextFile
    //      .map(line => (line.split("""\|""")(0), line.split("""\|""")(10)))
    //      .filter(_._2.substring(0, shipdate.length()) == shipdate)
    val part = sc.textFile(args.input() + "/part.tbl")
      .map(line => (line.split("""\|""")(0), line.split("""\|""")(1)))
     val partBroadcast=sc.broadcast(part.collectAsMap())
      
    val supplier = sc.textFile(args.input() + "/supplier.tbl")
      .map(line => (line.split("""\|""")(0), line.split("""\|""")(1)))
    val supplierBroadcast=sc.broadcast(supplier.collectAsMap())  

      
     

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      .map(line => (line, line.split("""\|""")(10)))
      .filter(_._2.substring(0, shipdate.length()) == shipdate)
      .map(pair => {
        val orderkey=pair._1.split("""\|""")(0)
        val partkey=pair._1.split("""\|""")(1)
        val suppkey=pair._1.split("""\|""")(2)
        val partTable=partBroadcast.value
        val supplierTable=supplierBroadcast.value
        (orderkey.toLong,(partTable.get(partkey),supplierTable.get(suppkey)))
      })
      .sortByKey(true)
      .take(20)
      
    lineitem.foreach(println)
      
      
      
      
//    val order = sc.textFile(args.input() + "/orders.tbl")
//      .map(line => (line.split("""\|""")(0), line.split("""\|""")(6)))
//
//    val orderitem = order.cogroup(lineitem)
//      .filter(_._2._2.size != 0)
//      .map(pair => (pair._1.toLong, pair._2._1.head))
//      .sortByKey(true)
//      .take(20)
//      .map(pair => (pair._2, pair._1))
//
//    orderitem.foreach(println)

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