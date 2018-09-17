package com.zhi

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark sql")
    val sc = new SparkContext(conf)
    val hctx = new HiveContext(sc)
  }
}
