package com.sparkDemo.examples

import org.apache.spark.sql.SparkSession

object ReadTextFileRDD {
  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(name = "Reading Text file")
      .master(master = "local[3]")
      .getOrCreate()

    //val rdd = spark.sparkContext.textFile(path = "D:\\Data Engg\\MyFirstSparkScalaProgram\\data\\textfile.txt")
    val rdd = spark.sparkContext.wholeTextFiles(path = "D:\\Data Engg\\MyFirstSparkScalaProgram\\data")


    rdd.foreach(println)
    //scala.io.StdIn.readLine;
  }

}
