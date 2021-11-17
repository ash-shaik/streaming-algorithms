package kafka-integration

import java.sql.Timestamp
import java.util

import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.scalacheck.Gen
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer, StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * We will be using scalacheck for data generation
  *
  */
object ClickStreamProducer {


  val spark = SparkSession.builder()
    .appName("ClickStream Kafka Producer")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[LongSerializer], // send data to kafka
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[LongDeserializer], // receiving data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  val kafkaTopic = "clickstream-analysis"
  val delay = 1

  val kafkaHashMap = new util.HashMap[String, Object]()
  kafkaParams.foreach { pair =>
    kafkaHashMap.put(pair._1, pair._2)
  }

  // producer can insert records into the Kafka topics
  // available on this executor
  val producer = new KafkaProducer[Long, String](kafkaHashMap)


  val batchSize = 1

  implicit val TimestampFormat : Encoder[Timestamp] with Decoder[Timestamp] = new Encoder[Timestamp] with Decoder[Timestamp] {
    //Spark represents interprets in seconds not milliseconds. Dividing the input by 1000
    override def apply(a: Timestamp): Json = Encoder.encodeLong.apply(a.getTime/1000)

    override def apply(c: HCursor): Result[Timestamp] = Decoder.decodeLong.map(s => new Timestamp(s)).apply(c)
  }


  def genBatchClicks() =
    for {
      events <- Gen.listOfN(batchSize, clickStreamAG.generateClickstreamData())
      dataBatch = events.map(_.asJson.noSpaces)
    } yield dataBatch

  def writeToKafka(): Unit = {
      genBatchClicks().sample.get.foreach { json =>
      println(json)
      val record = new ProducerRecord(kafkaTopic, System.currentTimeMillis(), json)
      producer.send(record)
    }
    Thread.sleep(delay)
    writeToKafka()
  }


  /**
    * Generate clickstream samples in batches of size(batchSize) 
    *
    * @param args
    */

  def main(args: Array[String]): Unit = {

    writeToKafka()
    ssc.start()
    ssc.awaitTermination()


  }


}
