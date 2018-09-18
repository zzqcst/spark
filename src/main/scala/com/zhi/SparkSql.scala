package com.zhi

import org.apache.spark.sql.SparkSession

object SparkSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("basic spark sql demo").getOrCreate()
    val df = spark.read.json("file:///home/hadoop/person.json")
  }
}
