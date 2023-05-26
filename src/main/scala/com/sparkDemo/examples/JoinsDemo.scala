package com.sparkDemo.examples

import org.apache.spark.sql.SparkSession

object JoinsDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame Demo")
      .master("local")
      .getOrCreate()
    val emp = Seq((1, "Sushmi", "2018", "10", "F", 3000),
      (2, "Rose", "2010", "20", "F", 4000),
      (3, "Theju", "2010", "10", "F", 1000),
      (4, "Jai", "2005", "10", "M", 2000),
      (5, "Sam", "2010", "40", "F", 3500),
      (6, "Sita", "2010", "50", "F", 5000)
    )
    val empColumns = Seq("emp_id", "name", "year_joined", "emp_dept_id", "gender", "salary")
    import spark.sqlContext.implicits._
    val empDF = emp.toDF(empColumns: _*)
    empDF.show(false)
    val dept = Seq(("Finance", 10),
      ("Marketing", 20),
      ("Sales", 30),
      ("IT", 40)
    )
    val deptColumns = Seq("dept_name", "dept_id")
    val deptDF = dept.toDF(deptColumns: _*)
    deptDF.show(false)


    //Inner join
    //println("Inner join")
    //empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner").show(false)
    //Outer join
    //empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "fullouter").show(false)
    //left outer
    val dem = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftouter").show(false)
    //right outer
    //empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "rightouter").show(false)
    //empDF.createOrReplaceTempView("EMP")
    //deptDF.createOrReplaceTempView("DEPT")
    //SQL JOIN
    //val joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id")
    //joinDF.show(false)
    //val joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id")
    //joinDF2.show(false)
    //empDF.groupBy("emp_dept_id").sum("salary").show(false)
  }
}
