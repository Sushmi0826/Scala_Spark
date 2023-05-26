package com.sparkDemo.examples

import org.apache.spark.sql.SparkSession

object RDDMapDemo {  //1 to 1 mapping
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(name = "Reading Text file")
      .master(master = "local[1]")
      .getOrCreate()
    val data = Seq("Project Gutenberg's",
      "Alice's Adventures in Wonderland",
      "Project Gutenberg's",
      "Adventures in Wonderland",
      "Project Gutenberg's")
    import spark.sqlContext.implicits._
    val df = data.toDF (colNames= "data")
    df.show()

    val mapDF = df.map(fun=> {
      fun.getString(0).split(" ")
    })
    mapDF.show()
  }
}

