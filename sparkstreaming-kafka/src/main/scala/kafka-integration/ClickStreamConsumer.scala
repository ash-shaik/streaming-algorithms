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


  case class ClickStream(user_id: Int,
                         device: String,
                         client_event: String,
                         client_timestamp: String)

  def readFromKafka() = {
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", kafkaTopic)
      .load()

    kafkaDF
      .select(from_json($"value".cast("string"), schema).as("data"))
      .select("data.*")
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  //TBC
  // Windowing Analysis


  def main(args: Array[String]): Unit = {
    readFromKafka()
  }


}
