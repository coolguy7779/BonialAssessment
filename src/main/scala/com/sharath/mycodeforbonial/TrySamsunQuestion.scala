package com.sharath.mycodeforbonial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

object TrySamsunQuestion extends  App{

  val spark = SparkSession.builder().master("local[2]").getOrCreate()
  val dataset = spark.read.format("csv").option("header", true).option("inferSchema", true)
    .load("D:\\LearningS\\samsun_interview\\dataset1.txt")
  val w = Window.partitionBy("col1","col2","col3","col4")
  val dfWithRowNum = dataset.withColumn("row_num", row_number() over w)
  dfWithRowNum.show(false)

}
