import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types._
import java.sql.Timestamp


case class CommonIpSchema(brochure_click_uuid: String,date_time: java.sql.Timestamp,page: Long, page_view_mode: String, event: String, duration: Long )

val spark: SparkSession = SparkSession.builder()
  .appName("Bonial_joins")
  .master("local[2]")
  .getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions","5")
import spark.implicits._

def rev(str: String): Boolean ={
//0(n)
  val temp = str.reverse
  if (temp == str) true
  else false

}
rev("9")
//
//
//
//
////def aggregateDurationsForEachPage(df: Dataset[Row]): Dataset[Row] = {
////
////  val durationAgg_per_UuidAndPage = df
////    .groupBy("brochure_click_uuid", "page")
////    .agg(sum("duration").alias("duration"))
////  durationAgg_per_UuidAndPage
////
////}
////
////val input = Array("0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:27:15|34  |DOUBLE_PAGE_MODE|PAGE_TURN |4",
////  "00555189-fdb4-434b-b072-83f9ae2221b1|2016-07-12 13:36:45|2   |SINGLE_PAGE_MODE|PAGE_TURN |4",
////  "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:27:05|32  |DOUBLE_PAGE_MODE|PAGE_TURN |4",
////  "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:27:13|32  |DOUBLE_PAGE_MODE|PAGE_TURN |2",
////  "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 06:59:28|15|SINGLE_PAGE_MODE|PAGE_TURN|28",
////  "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:27:09|34  |DOUBLE_PAGE_MODE|PAGE_TURN |4 ",
////  "00555189-fdb4-434b-b072-83f9ae2221b1|2016-07-12 13:36:23|2   |SINGLE_PAGE_MODE|PAGE_TURN |1")
////
////val inputDF = spark.sparkContext.parallelize(input).map(_.split('|'))
////  .map ( a => CommonIpSchema(a(0), Timestamp.valueOf(a(1)), a(2).trim.toLong, a(3), a(4),a(5).trim.toLong)).toDF().coalesce(1)
////
////
////aggregateDurationsForEachPage(inputDF).printSchema()
////
////
////val expected = Array("0017a3da-f18a-4fc5-957e-d9d12200d01c|34 |8",
////  "00555189-fdb4-434b-b072-83f9ae2221b1|2 |5",
////  "0017a3da-f18a-4fc5-957e-d9d12200d01c|32 |6",
////  "00180b74-93f1-4170-9cb2-8bd195f77951|15|28")
////
////case class DurationAggregationExpected(brochure_click_uuid: String, page: Long, duration: Option[Long])
//// //spark.sparkContext.parallelize(expected).map(_.split('|'))
//////  .map ( a => DurationAggregationExpected(a(0), a(1).trim.toLong,Option(a(2).trim.toLong))).toDF().coalesce(1).printSchema()
////
////
////val page_turns_read = spark.read
////  .format("json")
////  .option("inferSchema","true")
////  .load("D:\\Interview Preparation\\Bonial Interview Related\\exercise-S\\exercise-S\\page_turns.json")
////  .select(col("brochure_click_uuid"),
////    col("date_time")
////      .cast("timestamp"),col("page"),
////    col("page_view_mode"),col("event"))
////
////val enters_read = spark.read
////  .format("json")
////  .option("inferSchema","true")
////  .load("D:\\Interview Preparation\\Bonial Interview Related\\exercise-S\\exercise-S\\enters.json")
////  .select(col("brochure_click_uuid"), col("date_time")
////    .cast("timestamp"), col("page"),
////    col("page_view_mode"),col("event"))
////
////val exists_read = spark.read
////  .format("json")
////  .option("inferSchema","true")
////  .load("D:\\Interview Preparation\\Bonial Interview Related\\exercise-S\\exercise-S\\exits.json")
////  .select(col("brochure_click_uuid"), col("date_time")
////    .cast("timestamp"), col("page"),
////    col("page_view_mode"),col("event"))
////
//val brochure_click_read = spark.read.
//  format("json")
//  .option("inferSchema","true")
//  .load("D:\\Interview Preparation\\Bonial Interview Related\\exercise-S\\exercise-S\\brochure_clicks.json")
//  .select("brochure_id", "brochure_click_uuid")
////
////
////exists_read.printSchema()//.sort("brochure_click_uuid","date_time").show(350,false)
////
//brochure_click_read.printSchema()//.sort("brochure_click_uuid").show(350,false)
////
////
////
////
////
////
////
////
////
////
////
////
////
/////******************************************************/
//////
//////val schema = StructType(Array(StructField("brochure_click_uuid", StringType, true),
//////  StructField("date_time", TimestampType, true),
//////  StructField("page", LongType, true),
//////  StructField("page_view_mode", StringType, true),
//////  StructField("event", StringType, true)))
//////
//////val expectedDfSchema = schema.add("duration",LongType, true)
//////val expectedRows =  Seq(Row("0017a3da-f18a-4fc5-957e-d9d12200d01c", Timestamp.valueOf(" 2016-07-12 18:27:51"), 32L, "SINGLE_PAGE_MODE", "EXIT_VIEW", null ),
//////  Row("0017a3da-f18a-4fc5-957e-d9d12200d01c",Timestamp.valueOf("2016-07-12 18:26:22"), 8L, "DOUBLE_PAGE_MODE", "PAGE_TURN", 89L))
//////val expectedRDD = spark.sparkContext.parallelize(expectedRows)
//////val expectedDF = spark.createDataFrame(expectedRDD,expectedDfSchema)
//////expectedDF.printSchema()
//////
//////case class CommonIpSchema(brochure_click_uuid: String,date_time: java.sql.Timestamp,page: Long, page_view_mode: String, event: String, duration: Long )
//////
//////val input = Array("0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:14|1|SINGLE_PAGE_MODE|ENTER_VIEW|1",
//////  "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:18|2|DOUBLE_PAGE_MODE|PAGE_TURN |1",
//////  "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:19|4|DOUBLE_PAGE_MODE|PAGE_TURN |2",
//////  "017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:21|6|DOUBLE_PAGE_MODE|PAGE_TURN |1",
//////  "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:22|8|DOUBLE_PAGE_MODE|PAGE_TURN |",
//////  "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 06:59:28|1|SINGLE_PAGE_MODE|ENTER_VIEW|28",
//////  "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 06:59:56|2|SINGLE_PAGE_MODE|PAGE_TURN |"
//////)
//////
//////val expectedData = Array(
//////  "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:14|1|SINGLE_PAGE_MODE|ENTER_VIEW|1",
//////  "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:18|2|DOUBLE_PAGE_MODE|PAGE_TURN |1",
//////  "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:19|4|DOUBLE_PAGE_MODE|PAGE_TURN |2",
//////  "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:18|3|DOUBLE_PAGE_MODE|PAGE_TURN |1",
//////  "0017a3da-f18a-4fc5-957e-d9d12200d01c|2016-07-12 18:26:19|5|DOUBLE_PAGE_MODE|PAGE_TURN |2",
//////  "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 06:59:28|1 |SINGLE_PAGE_MODE|ENTER_VIEW|28",
//////  "00180b74-93f1-4170-9cb2-8bd195f77951|2016-07-13 06:59:56|2 |SINGLE_PAGE_MODE|PAGE_TURN |5"
//////
//////
//////)
//////
//////val inputDF = spark.sparkContext.parallelize(input).map(_.split('|'))
//////  .map ( a => CommonIpSchema(a(0), Timestamp.valueOf(a(1)), a(2).trim.toLong, a(3), a(4),a(5).trim.toLong)).toDF().coalesce(1)
//////
//////val expectedDF1 = spark.sparkContext.parallelize(expectedData).map(_.split('|'))
//////  .map ( a => CommonIpSchema(a(0), Timestamp.valueOf(a(1)), a(2).trim.toLong, a(3), a(4),a(5).trim.toLong)).toDF().coalesce(1)
////////val a = input.map(x => x.split('|'))
//////
////////input.dropRight()
//////
//////inputDF.show(false)
//////
//////
//////
////////
//////////val ts: Timestamp = new Timestamp()
////////
////////val str = """{"event": "PAGE_TURN", "brochure_click_uuid": "0092f04a-c2e7-40de-ad8c-e877ad887629", "date_time": "2016-07-13 01:16:37.0", "page_view_mode": "DOUBLE_PAGE_MODE", "page": 24, "user_agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0", "ip_address": "109.193.87.XX""""
////////
////////
////////val inputRow = Seq(Row("0017a3da-f18a-4fc5-957e-d9d12200d01c", Timestamp.valueOf(" 2016-07-12 18:27:51"), 32L, "SINGLE_PAGE_MODE", "EXIT_VIEW" ),
////////  Row("0017a3da-f18a-4fc5-957e-d9d12200d01c",Timestamp.valueOf("2016-07-12 18:26:22"), 8L, "DOUBLE_PAGE_MODE", "PAGE_TURN"))
////////
////////val inputRDD = spark.sparkContext.parallelize(inputRow)
////////val inputDF  =  spark.createDataFrame(inputRDD, Schema)
////////
//////////inputDF.show()
//////////inputDF.printSchema()
////////def calculateDuration(df: Dataset[Row]): Dataset[Row] ={
////////
////////  val windowSpec = Window.partitionBy("brochure_click_uuid").orderBy("date_time")
////////  val timeFmt = "yyyy-MM-dd'T'HH:mm:ss"
////////  val endTime = lead(col("date_time"),1).over(windowSpec)
////////  val startTime = col("date_time")
////////  val timeDiff = (unix_timestamp(endTime, timeFmt)
////////    - unix_timestamp(startTime, timeFmt))
////////  val duration = df
////////    .withColumn("duration", lit(when(col("event") === "EXIT_VIEW",lit(null)
////////      .cast("long"))
////////      .otherwise(timeDiff)))
////////    .where("""event != "EXIT_VIEW" """)
////////  duration
////////
////////}
////////
////////val result = calculateDuration(inputDF).where(col("date_time").isNotNull).select("duration").collect()(0).getLong(0)
////////result
////////
////////val expected = 89L
