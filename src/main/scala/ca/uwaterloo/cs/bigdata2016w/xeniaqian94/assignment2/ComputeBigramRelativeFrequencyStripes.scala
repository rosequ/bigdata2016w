package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment2


import ca.uwaterloo.cs.bigdata2016w.xeniaqian94.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import collection.mutable.HashMap

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
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
        val stripes=new HashMap[String,HashMap[String,Int]]()
        if(tokens.length > 1){
           val pairList=tokens.sliding(2).toIterator
           while(pairList.hasNext){           
             val p=pairList.next();
             if(stripes.contains(p(0))){
               var stripe = stripes(p(0))
               if (stripe.contains(p(1))) stripe+=(p(1) -> (stripe(p(1))+1)) else stripe+=(p(1) -> 1)
               stripes+=(p(0) -> stripe)
               
             }
             else{
               var stripe=new HashMap[String,Int]()
               stripe+=(p(1) -> 1)
               stripes+=(p(0) -> stripe)
             }
           }    
        }
        stripes.toList
      })
      .reduceByKey((a,b)=>a++(for((k,v)<- b) yield (k->(v+(if(a.contains(k)) a(k) else 0)))))
      .flatMap(a=>{       
        var marginal=a._2.values.sum
        for((k,v)<- a._2) yield (k->(1.0*v/marginal))       
      })
      .sortByKey()
    
    counts.saveAsTextFile(args.output())
  }
}