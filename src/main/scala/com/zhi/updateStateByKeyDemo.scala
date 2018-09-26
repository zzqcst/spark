package com.zhi

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * updateStateByKey操作
  * 在之前批次基础上计算
  */
object updateStateByKeyDemo {
    def main(args: Array[String]) {
        //定义状态更新函数
        val updateFunc = (values: Seq[Int], state: Option[Int]) => {
            val currentCount = values.foldLeft(0)(_ + _)
            val previousCount = state.getOrElse(0)
            Some(currentCount + previousCount)
        }
        StreamingExamples.setStreamingLogLevels() //设置log4j日志级别
        val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCountStateful")
        val sc = new StreamingContext(conf, Seconds(5))
        sc.checkpoint("file:///home/hadoop/mycode/kafka/checkpoint") //设置检查点，检查点具有容错机制
        val lines = sc.socketTextStream("localhost", 9999)
        val words = lines.flatMap(_.split(" "))
        val wordDstream = words.map(x => (x, 1))
        val stateDstream = wordDstream.updateStateByKey[Int](updateFunc)
        stateDstream.print()
        sc.start()
        sc.awaitTermination()
    }
}
