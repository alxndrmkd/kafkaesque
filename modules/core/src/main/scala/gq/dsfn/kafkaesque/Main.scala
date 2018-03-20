package gq.dsfn.kafkaesque

import cats.effect.IO
import fs2._
import gq.dsfn.kafkaesque.consumer.ConsumerSource
import gq.dsfn.kafkaesque.producer.ProducerSink
import gq.dsfn.kafkaesque.settings.{ConsumerSettings, ProducerSettings}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.duration._
import scala.util.Random

object Main extends App {

  implicit val eff = cats.effect.IO.ioEffect

  val props = Map(
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> s"Group${Random.nextLong()}"
  )

  val consumerConfig = ConsumerSettings(
    new StringDeserializer,
    new StringDeserializer,
    props
  )

  val producerConfig = ProducerSettings(
    new StringSerializer,
    new StringSerializer,
    props
  )

  val source: Stream[IO, ConsumerRecord[String, String]] =
    ConsumerSource(consumerConfig)
      .subscribe("TestIn")
      .plainStream(1 second)

  val sink: Sink[IO, String] =
    ProducerSink(producerConfig)
      .simpleSink("TestOut")

  source
    .map(_.value())
    .map(m => s"Processed, $m")
    .to(sink)
    .compile
    .drain
    .unsafeRunSync()

}
