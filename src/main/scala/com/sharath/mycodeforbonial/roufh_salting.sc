

import java.util.Random

import scala.math._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{exp => _, floor => _, _}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

val sparkSession: SparkSession = SparkSession.builder()
  .appName("Bonial_joins")
  .master("local[2]")
  .getOrCreate()
sparkSession.conf.set("spark.sql.shuffle.partitions","5")

val jsonPath = "D:\\Interview Preparation\\Bonial Interview Related\\salting.txt"

val jsonDfLeft = sparkSession.read.json(jsonPath)

//jsonDfLeft.show()
//jsonDfLeft.printSchema()

val saltedLeft = jsonDfLeft.rdd.flatMap(r => {
  val group = r.getAs[String]("group")
  val value = r.getAs[Long]("value")

  Seq((group + "_" + 0, value),(group + "_" + 1, value))
})

jsonDfLeft.rdd.collect()

/*
+-----+-----+
|group|value|
+-----+-----+
|    A|    1|
  |    A|    2|
  |    A|    3|
  |    A|    4|
  |    A|    5|
  |    B|    6|
  |    B|    7|
  |    B|    8|
  |    B|    9|
  |    B|    9|
  +-----+-----+*/


/*res2: Array[(String, Long)] = Array((A_0,1), (A_1,1), (A_0,2), (A_1,2), (A_0,3), (A_1,3), (A_0,4), (A_1,4), (A_0,5), (A_1,5), (B_0,6), (B_1,6), (B_0,7), (B_1,7), (B_0,8), (B_1,8), (B_0,9), (B_1,9), (B_0,9), (B_1,9))*/

val jsonDfRight = sparkSession.read.json(jsonPath)

val saltedRight = jsonDfRight.rdd.mapPartitions(it => {

  val random = new Random()

  it.map(r => {
    val group = r.getAs[String]("group")
    val value = r.getAs[Long]("value")

    (group + "_" + random.nextInt(2), value)
  })
})

//saltedRight.collect()

val num_parts = 16

//val skewed_large_rdd = sparkSession.sparkContext.parallelize( 0 until(num_parts), num_parts).flatMap(x =>  0 until exp(x).toInt)

//skewed_large_rdd.take(200)

val df = sparkSession.read.json("D:\\Interview Preparation\\Bonial Interview Related\\exercise-S\\exercise-S\\page_turns.json")

val saltedDF = df.withColumn("saltedKey",concat(col("brochure_click_uuid"),lit("_"), floor(rand(123456)*4)))
  .withColumn("brochure_click_uuid", split(col("saltedKey")(0), "_")(0))
saltedDF.show(5, false)