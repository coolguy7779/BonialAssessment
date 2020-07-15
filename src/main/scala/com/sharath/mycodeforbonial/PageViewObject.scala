package com.sharath.mycodeforbonial

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
  * This is Object is Main Transformation ProcessStep that -
  * -calculates User time-spent on each brochure page
  *
  * Inputs: 04 source Datafiles are used which are in JSON format
  * Output: Transformed output is stored as JSON
  */
object PageViewObject{

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = SparkSession.builder()
        .appName("Bonial_joins")
        .master("local[4]")
        .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions","10")
    val pageViewObject = combinedLogic(ReadJsonFiles
        .readEntersJsonFile(spark/*, args(0)*/),ReadJsonFiles
        .readPageTurnsJsonFile(spark/*, args(1)*/), ReadJsonFiles
        .readExistsJsonFile(spark/*, args(2)*/),
        ReadJsonFiles.readBrochureClickJson(spark/*, args(3)*/))
    //print(pageViewObject.rdd.toDebugString)
    //spark.time(writePageViewObject(pageViewObject))
    pageViewObject.sort("brochure_click_uuid","page").show(200,false)
  }

  /**
    * This method executes the complete logic needed to get Final Page View Object
    *
    * @return transformed Final Dataset for writing as pageViewObject
    */
  def combinedLogic(enters_read: Dataset[Row],pageTurn_read: Dataset[Row], exists_read:
    Dataset[Row],brochureClick_read: Dataset[Row]): Dataset[Row] ={

    val combinedDf = combineInputDF(pageTurn_read,enters_read,exists_read)
    val pageViewDuration = combinedDf
      .transform(calculateDuration)
      .transform(addOddPagesForDoubleViewModeEvents)
      .transform(removeRecordsWithNullDurations)
      .transform(aggregateDurationsForEachPage)
    //val pageViewObject = getBrochureID(pageViewDuration, brochureClick_read)
    //pageViewObject
    pageViewDuration
  }

  /**
    * This method combines records form following Inputs
    * Enters
    * Page_Turns
    * Exists
    *
    * @return combines records as combinedDF
    */
  def combineInputDF(df1: Dataset[Row], df2: Dataset[Row], df3: Dataset[Row]): Dataset[Row] ={

    val combinedDF = df1
      .union(df2)
      .union(df3)
    combinedDF
  }

  /**
    * This method calculates the duration for each page per user event
    * Removes exist records
    *
    * @return transformed output with duration as durationDF
    */
  def calculateDuration(df: Dataset[Row]): Dataset[Row] ={

    val windowSpec = Window.partitionBy("brochure_click_uuid").orderBy("date_time")
    val timeFmt = "yyyy-MM-dd'T'HH:mm:ss"
    val endTime = lead(col("date_time"),1).over(windowSpec)
    val startTime = col("date_time")
    val timeDiff = (unix_timestamp(endTime, timeFmt)
      - unix_timestamp(startTime, timeFmt))
    val durationDF = df
      .withColumn("duration", lit(when(col("event") === "EXIT_VIEW",lit(null)
        .cast("long"))
        .otherwise(timeDiff)))
      .where("""event != "EXIT_VIEW" """)
    durationDF
  }

  /**
    * This method adds odd pages with duration for each odd page per user event
    * Odd pages are added only for DoubleViewMode Records
    *
    *
    * @return transformed output with odd pages as duration_including_oddPageDF
    */

  def addOddPagesForDoubleViewModeEvents(df: Dataset[Row]) :Dataset[Row] = {

    val duration_doublePage_odd = df
      .where("""page_view_mode = "DOUBLE_PAGE_MODE" """)
      .withColumn("page_odd", col("page") + 1)
      .drop("page")
      .select(col("brochure_click_uuid"), col("date_time")
        .cast("timestamp"),col("page_odd")
        .alias("page"), col("page_view_mode"),
        col("event"),col("duration"))
    val duration_including_oddPageDF = df.union(duration_doublePage_odd)
    duration_including_oddPageDF
  }

  /**
    * This method removes records with NULL for duration
    *
    * @return transformed output without NULL in duration as duration_without_nullsDF
    */
  def removeRecordsWithNullDurations(df: Dataset[Row]): Dataset[Row] = {

    val duration_without_nullsDF = df
      .where(col("duration").isNotNull)
    duration_without_nullsDF
  }

  /**
    * This method calculates aggregate Duration for revisited pages by the same user
    *
    * @return transformed output as durationAgg_per_UuidAndPageDF
    */
  def aggregateDurationsForEachPage(df: Dataset[Row]): Dataset[Row] = {

//    val saltedDF = df.withColumn("saltedKey",concat(col("brochure_click_uuid"),lit("_"), floor(rand(123456)*19)))
    val saltedDF = df.withColumn("saltedKey",concat(col("brochure_click_uuid"),lit("_"), floor(rand(123456)*4)))
    val durationAgg_per_UuidAndPageDF = saltedDF
      .groupBy("saltedKey","page")
      .agg(sum("duration").alias("duration"))
      .withColumn("brochure_click_uuid", split(col("saltedKey"), "_")(0))

    durationAgg_per_UuidAndPageDF
  }

  /**
    * This method joins aggregated dataset and BrochureClick dataset
    *
    *
    * @return transformed output  as withBrochureIdDF
    */
  def getBrochureID(df: Dataset[Row], df1: Dataset[Row]): Dataset[Row] ={
    val withBrochureIdDF = df
      .join(df1, Seq("brochure_click_uuid"), "inner")
      .select("brochure_click_uuid", "brochure_id", "page","duration")
    withBrochureIdDF
  }

  /**
    * This method writes a dataframe as JSON file
    *
    */
  def writePageViewObject(pageViewObject: Dataset[Row]){

    pageViewObject
      .coalesce(1).
      sort(col("brochure_click_uuid"), col("page"))
      .write.mode(SaveMode.Overwrite).format("json")
      .save("hdfs:///user/xyzxxx9973375/output")
  }

}

/**
  * This is Object is helper object to read input datasets from Json files
  * Contains 4 methods to read 4 different input files
  *
  */
object ReadJsonFiles{

  /**
    * This is method reads page_turn.json
    *
    * @return exposes file page_turn.json as page_turns_readDF
    */
  def readPageTurnsJsonFile(spark :SparkSession, path: String = "D:\\Interview Preparation\\Bonial Interview Related\\exercise-S\\exercise-S\\page_turns.json"): Dataset[Row] ={
    val page_turns_readDF = spark.read
      .format("json")
      .option("inferSchema","true")
      .load(path)
      .select(col("brochure_click_uuid"),
        col("date_time")
          .cast("timestamp"),col("page"),
        col("page_view_mode"),col("event")).repartition(col("date_time"))
    page_turns_readDF
  }

  /**
    * This is method reads page_turn.json
    *
    * @return exposes file enters.json as enters_readDF
    */
  def readEntersJsonFile(spark :SparkSession, path :String = "D:\\Interview Preparation\\Bonial Interview Related\\exercise-S\\exercise-S\\enters.json"): Dataset[Row] ={

    val enters_readDF = spark.read
      .format("json")
      .option("inferSchema","true")
      .load(path)
      .select(col("brochure_click_uuid"), col("date_time")
        .cast("timestamp"), col("page"),
        col("page_view_mode"),col("event"))
    enters_readDF
  }

  /**
    * This is method reads page_turn.json
    *
    * @return exposes file exists.json as exists_readDF
    */
  def readExistsJsonFile(spark :SparkSession, path :String = "D:\\Interview Preparation\\Bonial Interview Related\\exercise-S\\exercise-S\\exits.json"): Dataset[Row] ={

    val exists_readDF = spark.read
      .format("json")
      .option("inferSchema","true")
      .load(path)
      .select(col("brochure_click_uuid"), col("date_time")
        .cast("timestamp"), col("page"),
        col("page_view_mode"),col("event"))
    exists_readDF
  }

  /**
    * This is method reads page_turn.json
    *
    * @return exposes file brochure_click.json as brochure_click_readDF
    */
  def readBrochureClickJson(spark :SparkSession, path :String = "D:\\Interview Preparation\\Bonial Interview Related\\exercise-S\\exercise-S\\brochure_clicks.json"): Dataset[Row] ={

    val brochure_click_readDF = spark.read.
      format("json")
      .option("inferSchema","true")
      .load(path)
      .select("brochure_id", "brochure_click_uuid")
    brochure_click_readDF
  }
}

