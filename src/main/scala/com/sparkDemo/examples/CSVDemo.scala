package com.sparkDemo.examples

import org.apache.spark.sql.SparkSession

object CSVDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV Demo")
      .master("local")
      .getOrCreate()
    val df = spark.read.option("header",true)
      .csv(path= "data/emp.csv")
    //df.show()
    //df.select("name","age").where(df("age") > "21").show()
    df.select("name","age").show()
    df.where(df("age") > "21").show()

  }
}
