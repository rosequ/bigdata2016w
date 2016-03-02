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

object Q5 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    println("hello world")
    //    BasicConfigurator.configure();

    log.info("Input: " + args.input())
//    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q5")
    //    System.setProperty("hadoop.home.dir", "/");

    val sc = new SparkContext(conf)

    val shipdate = args.date()

    val customer = sc.textFile(args.input() + "/customer.tbl")
      .map(line => (line.split("""\|""")(0), line.split("""\|""")(3).toInt))
    val customerBroadcast = sc.broadcast(customer.collectAsMap())

    val order = sc.textFile(args.input() + "/orders.tbl")
      .map(line => (line.split("""\|""")(0), line.split("""\|""")(1)))
      .map { pair =>
        {
          val customerTable = customerBroadcast.value
          customerTable.get(pair._2) match {
            case (Some(nationkey)) => (pair._1, nationkey)
          }
        }
      }
      .filter {
        pair => (pair._2 == 3 | pair._2 == 24)
      }

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      .map(line => (line.split("""\|""")(0), line.split("""\|""")(10).substring(0, 7)))
      .cogroup(order)
      .flatMap { pair =>
        {
          val shipdateList = pair._2._1.toList
          val nationkey = pair._2._2.head
          shipdateList.map(shipdate => ((shipdate, nationkey), 1)).toList
        }
      }
      .reduceByKey(_ + _)
    
    println("hello world 2")
    lineitem.collect().foreach(println)

//    lineitem.collect().foreach { pair =>
//      println("(" + pair._1 + "," + pair._2._1 + "," + pair._2._2 + ")")
//  }
  }

}
