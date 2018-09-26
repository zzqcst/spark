package com.zhi

import java.sql.{Connection, DriverManager, PreparedStatement}

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
//        stateDstream.saveAsTextFiles("file:///home/hadoop/res/dstreamoutput.txt")//DStream保存到文本文件
        stateDstream.foreachRDD(rdd => {
            //定义一个内部函数
            def func(records: Iterator[(String,Int)]) {
                var conn: Connection = null
                var stmt: PreparedStatement = null
                try {
                    val url = "jdbc:mysql://localhost:3306/spark"
                    val user = "root"
                    val password = "771929558"  //笔者设置的数据库密码是hadoop，请改成你自己的mysql数据库密码
                    conn = DriverManager.getConnection(url, user, password)
                    records.foreach(p => {
                        val sql = "insert into wordcount(word,count) values (?,?)"
                        stmt = conn.prepareStatement(sql)
                        stmt.setString(1, p._1.trim)
                        stmt.setInt(2,p._2.toInt)
                        stmt.executeUpdate()
                    })
                } catch {
                    case e: Exception => e.printStackTrace()
                } finally {
                    if (stmt != null) {
                        stmt.close()
                    }
                    if (conn != null) {
                        conn.close()
                    }
                }
            }

            val repartitionedRDD = rdd.repartition(3)//如果RDD分区数量太大，那么就会带来多次数据库连接开销
            repartitionedRDD.foreachPartition(func)
        })
        sc.start()
        sc.awaitTermination()
    }
}
