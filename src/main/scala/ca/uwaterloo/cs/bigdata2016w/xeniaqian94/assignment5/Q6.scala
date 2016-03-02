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

object Q6 extends Tokenizer {
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

    val shipdate = args.date()
    
    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      .map(line => (line, line.split("""\|""")(10)))
      .filter(_._2.substring(0, shipdate.length()) == shipdate)
      .map(line=>{
        val attribute=line._1.split("""\|""")
        val returnflag=attribute(8)
        val linestatus=attribute(9)
        val quantity=attribute(4).toLong
        val base_price=attribute(5).toDouble
        val discount=attribute(6).toDouble
        val tax=attribute(7).toDouble
        val disc_price=base_price*(1-discount)
        val charge=disc_price*(1+tax)
        ((returnflag,linestatus),(quantity,base_price,disc_price,charge,discount,1))
        
      })
      .reduceByKey((a, b) => (a._1+b._1,a._2+b._2,a._3+b._3,a._4+b._4,a._5+b._5,a._6+b._6))
      .sortByKey(true)
      .map(pair=>{
        val key=pair._1
        val sum_qty=pair._2._1
        val sum_base_price=pair._2._2
        val sum_disc_price=pair._2._3
        val sum_charge=pair._2._4
        val count_order=pair._2._6
        val avg_qty=sum_qty.toDouble/count_order
        val avg_price=sum_base_price/count_order
        val avg_disc=pair._2._5/count_order
        (key._1,key._2,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order)
      })
      
      lineitem.collect().foreach(println)
      
  }

}
