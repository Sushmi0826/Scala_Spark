package com.sparkDemo.examples

import org.apache.spark.sql.SparkSession

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(name = "Accumulator Demo")
      .master(master = "local[1]")
      .getOrCreate()

    val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")

    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7))

    //rdd.foreach(x => longAcc.add(x))
    rdd.foreach(x => longAcc.isZero)
    println(longAcc.value)
    scala.io.StdIn.readLine()

  }
}
