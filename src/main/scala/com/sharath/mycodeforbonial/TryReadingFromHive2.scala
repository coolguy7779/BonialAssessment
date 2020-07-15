package com.sharath.mycodeforbonial

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
object ReadingFromHive2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
//    val jdbcDF = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:hive2://localhost:10000")
//      .option("driver", "org.apache.hive.jdbc.HiveDriver")
//      .option("dbtable", "default.pokes")
//      /*.option("query", "select * from pokes")*/
//      .option("user", "")
//      .option("password", "")
//      .load()
    val sc = spark.sparkContext
    val textFile = sc.textFile("hdfs://localhost:9000/user/root/input/f1.txt")
    //val df = spark.read.text("hdfs://localhost:9000/user/root/input/f1.txt")
    textFile.collect().foreach(print)
    //df.show()


    //jdbcDF.printSchema()

  }

}
