package com.sharath.mycodeforbonial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

object TrySelfJoins extends  App{

  val spark = SparkSession.builder().master("local[2]").getOrCreate()
  val dataset = spark.read.format("csv").option("header", true).option("inferSchema", true)
    .load("C:\\Users\\myPC\\Documents\\mysql_dump_csv.csv")

  dataset.show(false)
 dataset.printSchema()

  val subDataset = dataset.select(dataset("Employee_Code").alias("Manager_Employee_Code"), dataset("Employee_Salary").alias("Manager_Employee_Salary"))
  val highSal = subDataset.join(dataset, dataset("Manager_Code") === subDataset("Manager_Employee_Code"), "inner")
    .where(dataset("Employee_Salary") > subDataset("Manager_Employee_Salary") )

  highSal.show(false)

}
