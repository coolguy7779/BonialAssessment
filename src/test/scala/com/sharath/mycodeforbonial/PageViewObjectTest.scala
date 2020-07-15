//package com.sharath.mycodeforbonial
//
//import java.sql.Timestamp
//
//import org.scalatest.{FunSpec, GivenWhenThen, Matchers}
//import com.holdenkarau.spark.testing._
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.types._
//import com.github.mrpowers.spark.fast.tests.DataFrameComparer
//import org.apache.log4j.Logger
//import org.apache.log4j.Level
//
//case class CommonIpSchema(brochure_click_uuid: String,date_time: java.sql.Timestamp,
//           page: Option[Long], page_view_mode: String, event: String)
//case class Brochure_clicks(brochure_id: Option[Long], brochure_click_uuid: String)
//case class CommonIpSchemaDuration(brochure_click_uuid: String,date_time: java.sql.Timestamp,
//           page: Long, page_view_mode: String, event: String, duration: Long )
//case class DurationAggregationExpected(brochure_click_uuid: String, page: Long,
//           duration: Option[Long])
//case class FinalExpectedSchema(brochure_click_uuid: String, brochure_id: Option[Long],
//           page: Option[Long], duration: Option[Long])
//
//class PageViewObjectTest extends FunSpec with Matchers with DataFrameSuiteBase
//      with DataFrameComparer with GivenWhenThen {
//
//  import spark.implicits._
//  it("calculates duration for each page viewed by users ") {
//    Logger.getLogger("org").setLevel(Level.WARN)
//    Given("Source dataframe ")
//    def schemaCommonToInputDataset(): StructType = {
//      val Schema = new StructType(Array(
//        StructField("brochure_click_uuid", StringType, true),
//        StructField("date_time", TimestampType, true),
//        StructField("page", LongType, true),
//        StructField("page_view_mode", StringType, true),
//        StructField("event", StringType, true)
//      ))
//      Schema
//    }
//    val inputRow = Seq(
//        Row("0017a3da-f18a-4fc5-957e-d9d12200d01c", Timestamp.valueOf(" 2016-07-12 18:27:51"),
//          32L, "SINGLE_PAGE_MODE", "EXIT_VIEW"),
//        Row("0017a3da-f18a-4fc5-957e-d9d12200d01c", Timestamp.valueOf("2016-07-12 18:26:22"),
//          8L, "DOUBLE_PAGE_MODE", "PAGE_TURN"))
//    val inputRDD = spark.sparkContext.parallelize(inputRow)
//    val inputDF = spark.createDataFrame(inputRDD, schemaCommonToInputDataset())
//
//    When("calculating duration")
//    val resultDF = PageViewObject.calculateDuration(inputDF)
//
//    Then("New dataframe has duration calculated in new column duration ")
//    val expectedDfSchema = inputDF.schema.add("duration", LongType, true)
//    val expectedRows = Seq(Row("0017a3da-f18a-4fc5-957e-d9d12200d01c",
//        Timestamp.valueOf("2016-07-12 18:26:22"), 8L, "DOUBLE_PAGE_MODE", "PAGE_TURN", 89L))
//    val expectedRDD = spark.sparkContext.parallelize(expectedRows)
//    val expectedDF = spark.createDataFrame(expectedRDD, expectedDfSchema)
//    assertSmallDataFrameEquality(expectedDF, resultDF, orderedComparison = false)
//  }
//
//  it("inserts odd pages for double view mode into the dataframe") {
//    Logger.getLogger("org").setLevel(Level.WARN)
//    Given("Source dataframe")
//    val input = Array(
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:14|1|SINGLE_PAGE_MODE|ENTER_VIEW|1",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:18|2|DOUBLE_PAGE_MODE|PAGE_TURN |1",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:19|4|DOUBLE_PAGE_MODE|PAGE_TURN |2",
//        "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 06:59:28|1|SINGLE_PAGE_MODE|ENTER_VIEW|28",
//        "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 06:59:56|2|SINGLE_PAGE_MODE|PAGE_TURN |5")
//    val inputDF = spark.sparkContext
//        .parallelize(input)
//        .map(_.split('|'))
//        .map(a => CommonIpSchemaDuration(a(0), Timestamp.valueOf(a(1)),
//          a(2).trim.toLong, a(3), a(4), a(5).trim.toLong))
//        .toDF()
//        .coalesce(1)
//
//    When("adding Odd Pages For Double View Mode Events")
//    val resultDF = PageViewObject.addOddPagesForDoubleViewModeEvents(inputDF)
//
//    Then("New dataframe contains rows with odd pages for double view mode")
//    val expectedData = Array(
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:14|1|SINGLE_PAGE_MODE|ENTER_VIEW|1",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:18|2|DOUBLE_PAGE_MODE|PAGE_TURN |1",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:19|4|DOUBLE_PAGE_MODE|PAGE_TURN |2",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:18|3|DOUBLE_PAGE_MODE|PAGE_TURN |1",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:19|5|DOUBLE_PAGE_MODE|PAGE_TURN |2",
//        "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 06:59:28|1 |SINGLE_PAGE_MODE|ENTER_VIEW|28",
//        "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 06:59:56|2 |SINGLE_PAGE_MODE|PAGE_TURN |5")
//    val expectedDF = spark.sparkContext
//        .parallelize(expectedData)
//        .map(_.split('|'))
//        .map(a => CommonIpSchemaDuration(a(0), Timestamp.valueOf(a(1)),
//          a(2).trim.toLong, a(3), a(4), a(5).trim.toLong))
//        .toDF()
//        .coalesce(1)
//    assertSmallDataFrameEquality(resultDF, expectedDF, orderedComparison = false)
//  }
//
//  it("aggregates duration for pages revisited by a user") {
//    Logger.getLogger("org").setLevel(Level.WARN)
//    Given( "Source Dataframe")
//    val input = Array(
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:27:15|34  |DOUBLE_PAGE_MODE|PAGE_TURN |4",
//        "00555189-fdb4-434b-b072-83f9ae2221b1|2016-07-12 13:36:45|2   |SINGLE_PAGE_MODE|PAGE_TURN |4",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:27:05|32  |DOUBLE_PAGE_MODE|PAGE_TURN |4",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:27:13|32  |DOUBLE_PAGE_MODE|PAGE_TURN |2",
//        "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 06:59:28|15|SINGLE_PAGE_MODE|PAGE_TURN|28",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:27:09|34  |DOUBLE_PAGE_MODE|PAGE_TURN |4 ",
//        "00555189-fdb4-434b-b072-83f9ae2221b1|2016-07-12 13:36:23|2   |SINGLE_PAGE_MODE|PAGE_TURN |1")
//    val inputDF = spark.sparkContext
//        .parallelize(input)
//        .map(_.split('|'))
//        .map(a => CommonIpSchemaDuration(a(0), Timestamp.valueOf(a(1)),
//          a(2).trim.toLong, a(3), a(4), a(5).trim.toLong))
//        .toDF()
//        .coalesce(1)
//
//    When ("Aggregating Durations for Each Page")
//    val resultDF = PageViewObject.aggregateDurationsForEachPage(inputDF)
//
//    Then("The duration for page revisits by a user is aggregated")
//    val expected = Array(
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|34 |8",
//        "00555189-fdb4-434b-b072-83f9ae2221b1|2 |5",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|32 |6",
//        "00180b74-93f1-4170-9cb2-8bd195f77951|15|28")
//    val expectedDF = spark.sparkContext
//        .parallelize(expected)
//        .map(_.split('|'))
//        .map(a => DurationAggregationExpected(a(0),
//          a(1).trim.toLong, Option(a(2).trim.toLong)))
//        .toDF()
//        .coalesce(1)
//    assertSmallDataFrameEquality(resultDF, expectedDF, orderedComparison = false)
//  }
//
//  it("creates a page view object with time spent on a page by a user"){
//    Logger.getLogger("org").setLevel(Level.WARN)
//    Given("four data sources as dataframes ")
//    val page_turn_input = Array(
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:23|10  |DOUBLE_PAGE_MODE|PAGE_TURN",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:25|12  |DOUBLE_PAGE_MODE|PAGE_TURN",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:29|14  |DOUBLE_PAGE_MODE|PAGE_TURN",
//        "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 07:00:24|11  |SINGLE_PAGE_MODE|PAGE_TURN",
//        "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 07:00:25|12  |SINGLE_PAGE_MODE|PAGE_TURN",
//        "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 07:00:26|11  |SINGLE_PAGE_MODE|PAGE_TURN",
//        "008421b4-ae52-45c3-b1d6-408e269d573e|2016-07-12 11:33:08|26  |DOUBLE_PAGE_MODE|PAGE_TURN",
//        "008421b4-ae52-45c3-b1d6-408e269d573e|2016-07-12 11:33:08|22  |DOUBLE_PAGE_MODE|PAGE_TURN",
//      "008421b4-ae52-45c3-b1d6-408e269d573e|2016-07-12 11:33:10|28  |DOUBLE_PAGE_MODE|PAGE_TURN")
//    val enters_input = Array(
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:14|1|SINGLE_PAGE_MODE|ENTER_VIEW",
//        "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 06:59:28|1   |SINGLE_PAGE_MODE|ENTER_VIEW",
//        "008421b4-ae52-45c3-b1d6-408e269d573e|2016-07-12 11:32:44|0   |DOUBLE_PAGE_MODE|ENTER_VIEW")
//    val exists_input = Array(
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:15|32  |SINGLE_PAGE_MODE|EXIT_VIEW",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:27:51|41  |SINGLE_PAGE_MODE|EXIT_VIEW",
//        "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 08:20:12|12  |SINGLE_PAGE_MODE|EXIT_VIEW",
//        "008421b4-ae52-45c3-b1d6-408e269d573e|2016-07-12 11:33:51|28  |DOUBLE_PAGE_MODE|EXIT_VIEW")
//    val brochure_click_input = Array(
//        "579376804|0017a3da-f18a-4fc5-957e-d9d12200d01c",
//        "581325976|00180b74-93f1-4170-9cb2-8bd195f77951",
//        "579376804|008421b4-ae52-45c3-b1d6-408e269d573e")
//    val page_turnDF = spark.sparkContext
//        .parallelize(page_turn_input).map(_.split('|'))
//        .map ( a => CommonIpSchema(a(0), Timestamp.valueOf(a(1)),
//          Option(a(2).trim.toLong), a(3), a(4)))
//        .toDF()
//        .coalesce(1)
//    val enters_turnDF = spark.sparkContext.
//        parallelize(enters_input).map(_.split('|'))
//        .map ( a => CommonIpSchema(a(0), Timestamp.valueOf(a(1)),
//          Option(a(2).trim.toLong), a(3), a(4)))
//        .toDF()
//        .coalesce(1)
//    val exits_turnDF = spark.sparkContext
//        .parallelize(exists_input).map(_.split('|'))
//        .map ( a => CommonIpSchema(a(0), Timestamp.valueOf(a(1)),
//          Option(a(2).trim.toLong), a(3), a(4)))
//        .toDF()
//        .coalesce(1)
//    val brochure_clickDF = spark.sparkContext
//        .parallelize(brochure_click_input).map(_.split('|'))
//        .map(a => Brochure_clicks(Option(a(0).trim.toLong), a(1))).toDF()
//        .coalesce(1)
//
//    When ("combine Logic is applied to 4 input dataframes")
//    val resultDF  = PageViewObject.combinedLogic(enters_turnDF,page_turnDF,
//      exits_turnDF, brochure_clickDF)
//
//    Then("page view object with duration per page view is created")
//    val expected_input = Array(
//        "00180b74-93f1-4170-9cb2-8bd195f77951|581325976  |12  |1",
//        "00180b74-93f1-4170-9cb2-8bd195f77951|581325976  |1   |56",
//        "00180b74-93f1-4170-9cb2-8bd195f77951|581325976  |11  |4787",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|579376804  |10  |2",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|579376804  |13  |4",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|579376804  |1   |1",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|579376804  |14  |82",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|579376804  |12  |4",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|579376804  |11  |2",
//        "0017a3da-f18a-4fc5-957e-d9d12200d01c|579376804  |15  |82",
//        "008421b4-ae52-45c3-b1d6-408e269d573e|579376804  |22  |2",
//        "008421b4-ae52-45c3-b1d6-408e269d573e|579376804  |1   |24",
//        "008421b4-ae52-45c3-b1d6-408e269d573e|579376804  |27  |0 ",
//        "008421b4-ae52-45c3-b1d6-408e269d573e|579376804  |23  |2 ",
//        "008421b4-ae52-45c3-b1d6-408e269d573e|579376804  |29  |41",
//        "008421b4-ae52-45c3-b1d6-408e269d573e|579376804  |0   |24",
//        "008421b4-ae52-45c3-b1d6-408e269d573e|579376804  |28  |41",
//        "008421b4-ae52-45c3-b1d6-408e269d573e|579376804  |26  |0 ")
//    val expectedDF = spark.sparkContext.
//        parallelize(expected_input)
//        .map(_.split('|'))
//        .map ( a => FinalExpectedSchema(a(0), Option(a(1).trim.toLong) ,
//          Option(a(2).trim.toLong), Option(a(3).trim.toLong)))
//        .toDF()
//        .coalesce(1)
//    assertSmallDataFrameEquality(resultDF, expectedDF, orderedComparison = false)
//  }
//
//}
