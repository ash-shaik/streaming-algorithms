package kafka-integration

import model.{Clickstream, User}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr

object StreamingJoin {


  val spark = SparkSession.builder()
    .appName("Clicks And Movements Stream Joiner")
    .master("local[2]")
    .getOrCreate()

  import org.apache.spark.sql.Encoders


  val kafkaTopic1 = "clickstream-analysis"
  val kafkaTopic2 = "user-movement"

  val clickstreamSchema = Encoders.product[Clickstream].schema
  val userMovementSchema = Encoders.product[User].schema

  def readUsermovementWithClickstream() = {
    clickstreamSchema.printTreeString()
    userMovementSchema.printTreeString()

    val clickstreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", kafkaTopic1)
      .option("starting/offsets", "earliest") //which will read all data available in the topic at the start of the query.
      //default value of “latest” is used and only data that arrives after the query starts will be processed
      .load()

    val clicksDF =
      clickstreamDF
        .select(from_json(col("value").cast("string"), clickstreamSchema,
          Map("timestampFormat" -> "uuuu-MM-dd HH:mm:ss.SSS")).alias("clicks"))

    val clicksDF1 = clicksDF.select("clicks.*")


    val usermovementDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", kafkaTopic2)
      .option("starting/offsets", "earliest") //which will read all data available in the topic at the start of the query.
      //default value of “latest” is used and only data that arrives after the query starts will be processed
      .load()

    val movementsDF =
      usermovementDF
        .select(from_json(col("value").cast("string"), userMovementSchema,
          Map("timestampFormat" -> "uuuu-MM-dd HH:mm:ss.SSS")).alias("movements"))

    val movementsDF1 = movementsDF.select("movements.*")


    val clicksWatermarkDF = clicksDF1.withWatermark("client_timestamp", "1 hour")
    val movementsWatermarkDF = movementsDF1.withWatermark("user_timestamp", "2 hour")

    val clicksWithMovementDF = movementsWatermarkDF.join(clicksWatermarkDF,
      expr("click_location = location AND client_timestamp >= user_timestamp AND client_timestamp <= user_timestamp + interval 1 hour"))

    clicksWithMovementDF.writeStream
        .format("console")
        .outputMode("append")
        .start()
        .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    readUsermovementWithClickstream()
  }


}
