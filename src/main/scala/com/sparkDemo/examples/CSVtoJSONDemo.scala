package com.sparkDemo.examples

import org.apache.spark.sql.{SaveMode, SparkSession}

object CSVtoJSONDemo {
  def main (args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSVtoJSON Demo")
      .master("local")
      .getOrCreate()
    val dfFromCSV = spark.read.option("header",true)
      .csv("data/emp.csv")
    dfFromCSV.printSchema()
    dfFromCSV.write.json("data/sampleJson")
    //dfFromCSV.show(false)
    //dfFromCSV.write.mode(SaveMode.Overwrite).json("data/sampleJson")
  }
}
