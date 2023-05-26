package com.sparkDemo.examples
import org.apache.spark.sql.SparkSession

object CreateRDD {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(name= "MyFirstRDD")
      .master( master = "local[3]")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Seq(("java",1000),("python",2000),("scala",500)))
    //val rdd = spark.sparkContext.textFile(path = "data/textFile")

    rdd.foreach(println)
    scala.io.StdIn.readLine;

  }
}
