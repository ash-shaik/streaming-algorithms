package kafka-integration

import model.Clickstream
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object ClickStreamConsumer {

  val spark = SparkSession.builder()
    .appName("ClickStream Kafka Consumer")
    .master("local[2]")
    .getOrCreate()

  import spark.sqlContext.implicits._
  import org.apache.spark.sql.Encoders


  val kafkaTopic = "clickstream-analysis"

  val schema = Encoders.product[Clickstream].schema


  /**
    * User Session Analysis - to build a dataframe of the below foramt.
    * -------------------------------------------
    * Batch:
    * -------------------------------------------
    * +-------+--------+-------+--------------------+-----------+----------------+--------------+-------------------+-------------------+----------------+
    * |user_id|  device|session|              window|event_count|begin_navigation|end_navigation|      begin_session|        end_session|        duration|
    * +-------+--------+-------+--------------------+-----------+----------------+--------------+-------------------+-------------------+----------------+
    * |     48|  tablet| 48 tab|[2021-11-16 19:40...|         17|           steps|          food|2021-11-16 19:43:15|2021-11-16 19:44:20|1.083333 seconds|
    * |     83|  tablet| 83 tab|[2021-11-16 19:40...|         12|           sleep|          food|2021-11-16 19:43:18|2021-11-16 19:44:20|1.033333 seconds|
    * |    180|  mobile|180 mob|[2021-11-16 19:40...|         15|           heart|          food|2021-11-16 19:43:16|2021-11-16 19:44:13|    0.95 seconds|
    * |    181|computer|181 com|[2021-11-16 19:40...|         16|           sleep|         steps|2021-11-16 19:43:20|2021-11-16 19:44:20|       1 seconds|
    * |    186|computer|186 com|[2021-11-16 19:40...|         16|           steps|         steps|2021-11-16 19:43:18|2021-11-16 19:44:19|1.016667 seconds|
    * |    203|  mobile|203 mob|[2021-11-16 19:40...|         12|           steps|         sleep|2021-11-16 19:43:17|2021-11-16 19:44:20|    1.05 seconds|
    * |    231|computer|231 com|[2021-11-16 19:40...|         14|           sleep|         heart|2021-11-16 19:43:22|2021-11-16 19:44:15|0.883333 seconds|
    * |    315|computer|315 com|[2021-11-16 19:40...|         13|           steps|          food|2021-11-16 19:43:21|2021-11-16 19:44:17|0.933333 seconds|
    * |    326|  tablet|326 tab|[2021-11-16 19:40...|         14|           heart|          food|2021-11-16 19:43:14|2021-11-16 19:44:14|       1 seconds|
    * |    381|computer|381 com|[2021-11-16 19:40...|         22|           steps|         steps|2021-11-16 19:43:23|2021-11-16 19:44:20|    0.95 seconds|
    *
    */

  def readFromKafka() = {

    schema.printTreeString()

    // streaming DataFrame subscribed to the topic

    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", kafkaTopic)
      .option("starting/offsets", "earliest") //which will read all data available in the topic at the start of the query.
      //default value of “latest” is used and only data that arrives after the query starts will be processed
      .load()

    //we’ll start by parsing the binary values present in the key and value columns.
    // by supplying the schema and timestamp format.
    val valueDF =
    kafkaDF
      .select(from_json(col("value").cast("string"), schema,
        Map("timestampFormat" -> "uuuu-MM-dd HH:mm:ss.SSS")).alias("clicks"))

    // We are transforming the incoming stream to build user sessions per device type
    // and perform low-latency event-time aggregation to summarize what user sessions look like.

    val clickstreamDF = valueDF.select("clicks.*")


    val clicksSummaryDF = clickstreamDF
      .withWatermark("client_timestamp", "10 minutes")
      .withColumn("session", concat($"user_id", lit(" ")
        , substring($"device", 0, 3)))
      .groupBy($"user_id", $"device", $"session"
        , window($"client_timestamp", "10 minutes"))
      .agg(count($"user_id").as("event_count"),
        first($"client_event").as("begin_navigation"),
        last($"client_event").as("end_navigation"),
        min($"client_timestamp").as("begin_session"),
        max($"client_timestamp").as("end_session"),
        round((max($"client_timestamp") - min($"client_timestamp")) / 60, 2) as "duration"
      )

    clicksSummaryDF
      .writeStream.format("console")
      .outputMode("complete").start().awaitTermination()


  }


  def main(args: Array[String]): Unit = {
    readFromKafka()
  }


}
