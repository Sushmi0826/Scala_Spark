package com.sparkDemo.examples

import org.apache.spark.sql.{SaveMode, SparkSession}

object CSVtoParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSVtoParquet")
      .master("local")
      .getOrCreate()
    val dfFromCSV = spark.read.option("header", true)
      .csv("data/emp.csv")
    dfFromCSV.printSchema()
    //convert to parquet
    dfFromCSV.write.parquet("data/employee1.parquet")
  }
}
