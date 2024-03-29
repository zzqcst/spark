package com.zhi

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object KafkaWordCount {
    def main(args: Array[String]) {
        StreamingExamples.setStreamingLogLevels()
        val sc = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
        val ssc = new StreamingContext(sc, Seconds(10))
        ssc.checkpoint("file:///home/hadoop/mycode/kafka/checkpoint") //设置检查点，如果存放在HDFS上面，则写成类似ssc.checkpoint("/user/hadoop/checkpoint")这种形式，但是，要启动hadoop
        val zkQuorum = "localhost:2181" //Zookeeper服务器地址
        val group = "1" //topic所在的group，可以设置为自己想要的名称，比如不用1，而是val group = "test-consumer-group"
        val topics = "wordsender" //topics的名称
        val numThreads = 1 //每个topic的分区数
        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap//(topic,1)
        val lineMap = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)//(主题，消息)
        val lines = lineMap.map(_._2)
        val words = lines.flatMap(_.split(" "))
        val pair = words.map(x => (x, 1))
        val wordCounts = pair.reduceByKeyAndWindow(_ + _, _ - _, Minutes(2), Seconds(10), 2) //这行代码的含义在下一节的窗口转换操作中会有介绍
        wordCounts.print()
        ssc.start
        ssc.awaitTermination
    }
}
