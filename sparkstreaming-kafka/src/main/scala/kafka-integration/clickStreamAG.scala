package kafka-integration

import model._

import org.scalacheck.Gen
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.ZoneOffset.UTC


object clickStreamAG {


  def generateClickstreamData(): Clickstream = {

    val rangeStart = LocalDateTime.now(UTC).minusMonths(1).toEpochSecond(UTC)
    val rangeEnd = LocalDateTime.now(UTC).toEpochSecond(UTC)

    val myTimestamp = {
      localDateTimeGen(rangeStart, rangeEnd)
    }

    val genClickStreamSample: Gen[Clickstream] = for {
      user_idg <- Gen.choose(1, 10000)
      deviceg <- Gen.oneOf("mobile", "computer", "tablet")
      client_eventg <- Gen.oneOf("steps", "sleep", "food", "heart")
      client_timestampg <- myTimestamp
    } yield Clickstream(
      user_id = user_idg,
      device = deviceg,
      client_event = client_eventg,
      client_timestamp = client_timestampg
    )

    //println(genClickStreamSample.sample.get.client_timestamp.getClass)
    //print(genClickStreamSample.sample.get)
    genClickStreamSample.sample.get

  }

  //Testing
  def main(args: Array[String]): Unit = {
    generateClickstreamData()
  }

  def localDateTimeGen(
                        rangeStart: Long
                        , rangeEnd: Long
                      ): Gen[LocalDate] = {
    Gen.const(Gen.choose(rangeStart, rangeEnd)
      .map(i => LocalDateTime.ofEpochSecond(i, 0, UTC))
      .sample.get.toLocalDate)
  }

}



