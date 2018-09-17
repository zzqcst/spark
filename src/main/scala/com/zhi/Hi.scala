package com.zhi
import org.apache.spark.{SparkConf, SparkContext}
object Hi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")
    val sc = new SparkContext(conf)
    val input = sc.textFile("file:///home/hadoop/a.txt")
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    counts.saveAsTextFile("wc_res")
    sc.stop()
  }
}
