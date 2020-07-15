package com.sharath.mycodeforbonial
import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types.StructType
import org.apache.log4j.Logger
import org.apache.log4j.Level

//Window functions in spark
object Rolling_or_Incremental_sum {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  val w = Window.partitionBy("country").orderBy("date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val df =  spark.read.csv("")
  df.withColumn("rank", rank().over(w)).filter("rank = 1")
//  val schema = new StructType().add("orders", Array(StructType.))
//  val df1 = spark.read.format("json").schema("schema").load("path").select(
//    explode("orders")
//  )



}
