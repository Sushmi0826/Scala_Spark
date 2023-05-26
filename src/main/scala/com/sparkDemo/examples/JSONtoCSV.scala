package com.sparkDemo.examples

import org.apache.spark.sql.{SaveMode, SparkSession}

object JSONtoCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JSONtoCSV")
      .master("local")
      .getOrCreate()
    //read json file into dataframe
    val df = spark.read.json("data/employee.json")
    df.printSchema()
    df.show(false)
    //convert to csv
    df.write.option("header", "true").csv("data/employee.csv")
  }
}
