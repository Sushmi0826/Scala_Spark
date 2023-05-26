package com.sparkDemo.examples

import org.apache.spark.sql.SparkSession

object BroadCastDemo {
  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(name = "Broadcast Demo")
      .master(master = "local[1]")
      .getOrCreate()

    val inputRdd = spark.sparkContext.parallelize(Seq(("Emp1","1000","USA","NY"),("Emp2","2000","IND","TN")
      ,("Emp3","40000","IND","TS"),("Emp4","4500","AUS","CA")))
    inputRdd.foreach(println)

    val states = Map(("NY", "New York"), ("CA", "California"), ("TN", "TamilNadu"),("TS","Telangana"))
    val countries = Map(("USA", "United States of America"), ("IND", "India"),("AUS","Australia"))

    val broadcastStates = spark.sparkContext.broadcast(states)
    val broadcastCountries = spark.sparkContext.broadcast(countries)
    val FinalRdd = inputRdd.map(f => {
      val country = f._3
      val state = f._4
      val fullCountry = broadcastCountries.value.get(country).get
      val fullState = broadcastStates.value.get(state).get
      (f._1, f._2, fullCountry, fullState)
    })
    println(FinalRdd.collect().mkString("\n"))
    scala.io.StdIn.readLine()
  }
}
