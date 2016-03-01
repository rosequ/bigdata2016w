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
      
    lineitem.foreach{pair=>
      pair match{
        case (orderkey,(Some(partkey),Some(suppkey))) => println("("+orderkey+","+partkey+","+suppkey+")")
        case _ =>  println()
      }
    }

  }

}