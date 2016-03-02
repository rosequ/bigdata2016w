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

object Q7 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    println("hello world")
    //    BasicConfigurator.configure();

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q6")
    //    System.setProperty("hadoop.home.dir", "/");

    val sc = new SparkContext(conf)

    val limitdate = args.date()

    val customer = sc.textFile(args.input() + "/customer.tbl")
      .map(line => (line.split("""\|""")(0), line.split("""\|""")(1)))
    val customerBroadcast = sc.broadcast(customer.collectAsMap())

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      .map(line => (line, line.split("""\|""")(10)))
      .filter(_._2 > limitdate)
      .map(pair => {
        val orderkey = pair._1.split("""\|""")(0).toLong
        val extendedprice = pair._1.split("""\|""")(5).toDouble
        val discount = pair._1.split("""\|""")(6).toDouble
        val singlerevenue = extendedprice * (1 - discount)
        (orderkey, singlerevenue)
      })
      .reduceByKey(_ + _)

    val order = sc.textFile(args.input() + "/orders.tbl")
      .map(line => (line, line.split("""\|""")(4)))
      .filter(_._2 < limitdate)
      .map(pair => {
        val orderkey = pair._1.split("""\|""")(0).toLong
        val customerTable = customerBroadcast.value
        val name = customerTable.get(pair._1.split("""\|""")(1))
        val orderdate = pair._2
        val shippriority = pair._1.split("""\|""")(7)
        (orderkey, (name, orderdate, shippriority))
      })

    val shippingpriority = lineitem.cogroup(order)
      .filter { pair => (pair._2._1.size != 0 & pair._2._2.size != 0) }
      .map(pair => {
        val name = {
          pair._2._2.head._1 match {
            case Some(name) => name
          }
        }
        val orderkey = pair._1
        val revenue = pair._2._1.head
        val orderdate = pair._2._2.head._2
        val shippriority = pair._2._2.head._3
        (revenue, (name, orderkey, orderdate, shippriority))

      })
      .sortByKey(false)
      .map(pair => (pair._2._1, pair._2._2, pair._1, pair._2._3, pair._2._4))

    shippingpriority.collect().foreach(println)

  }

}
