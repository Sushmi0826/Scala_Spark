package com.sparkDemo.examples

import org.apache.spark.sql.SparkSession

object DobleAccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DoubleAccumulatorDemo")
      .master("local")
      .getOrCreate()

    val DoubleAcc = spark.sparkContext.doubleAccumulator("DoubleAccumulator")

    val rdd = spark.sparkContext.parallelize(Array(1, 2.55, 3.999))

    rdd.foreach(x => DoubleAcc.add(x))

    println(DoubleAcc.value)
    scala.io.StdIn.readLine()
  }
}
