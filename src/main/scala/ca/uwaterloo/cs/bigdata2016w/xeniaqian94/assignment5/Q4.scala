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

object Q4 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    println("hello world")
    //    BasicConfigurator.configure();

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q4")
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
    val customer = sc.textFile(args.input() + "/customer.tbl")
      .map(line => (line.split("""\|""")(0), line.split("""\|""")(3)))
    val customerBroadcast = sc.broadcast(customer.collectAsMap())

    val nation = sc.textFile(args.input() + "/nation.tbl")
      .map(line => (line.split("""\|""")(0), line.split("""\|""")(1)))
    val nationBroadcast = sc.broadcast(nation.collectAsMap())

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      .map(line => (line.split("""\|""")(0), line.split("""\|""")(10)))
      .filter(_._2.substring(0, shipdate.length()) == shipdate)
      .map(pair => (pair._1, 1))
      .reduceByKey(_ + _)

    val order = sc.textFile(args.input() + "/orders.tbl")
      .map(line => (line.split("""\|""")(0), line.split("""\|""")(1)))

    val linenation = lineitem.cogroup(order)
      .filter(_._2._1.size != 0)
      .map(pair => {
        val customerTable = customerBroadcast.value
        (customerTable.get(pair._2._2.head), pair._2._1.head)
      })
      .reduceByKey(_ + _)
      .map { pair =>
        pair match {
          case (Some(nationkey), count) => {
            val nationTable = nationBroadcast.value
            (nationkey.toInt, count)
          }
        }
        
      }
      .map(pair=>(pair._1.toInt,pair._2))
      .sortByKey(true,1)
      .foreach(println)
      
      
//      nationTable.get(nationkey)
//    linenation.foreach(println)
//    linenation.foreach { pair =>
//      println("(" + pair._1 + "," + pair._2._1 + "," + pair._2._2 + ")")
//
//    }
  }

}
