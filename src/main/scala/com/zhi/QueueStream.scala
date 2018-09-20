package com.zhi
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
object QueueStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TestRDDQueue").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val rddQueue =new scala.collection.mutable.SynchronizedQueue[RDD[Int]]()
    val queueStream = ssc.queueStream(rddQueue)//创建基于RDD队列的DStream
    val mappedStream = queueStream.map(r => (r % 10, 1))//余数为0-9
    val reducedStream = mappedStream.reduceByKey(_ + _)//每个区间有十个数
    reducedStream.print()
    ssc.start()
    for (i <- 1 to 10){
      rddQueue += ssc.sparkContext.makeRDD(1 to 100,2)//每次产生一个1到100的数字序列,间隔为2
      Thread.sleep(1000)
    }
    ssc.stop()
  }
}
