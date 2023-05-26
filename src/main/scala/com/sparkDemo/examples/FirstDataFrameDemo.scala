package com.sparkDemo.examples

import org.apache.spark.sql.SparkSession

object FirstDataFrameDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame Demo")
      .master("local")
      .getOrCreate()
    val data = Seq(("Sushmi","100",21,"TamilNadu"),("Hema","1000",20,"Trichy"),("Tamizh","2000",31,"Chennai"),
      ("Sita","3000",24,"Tripur"))
    val columns = Seq("name","salary","age","place")
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(data)
    val df = rdd.toDF(columns:_*)
    df.show()
  }
}
