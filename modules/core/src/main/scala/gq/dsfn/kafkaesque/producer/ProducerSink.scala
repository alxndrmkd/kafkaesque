package gq.dsfn.kafkaesque.producer

import java.util.Properties

import cats.effect.{Async, Effect}
import fs2._
import gq.dsfn.kafkaesque.settings.ProducerSettings
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConverters._

final case class ProducerSink[F[_]: Async, K, V](config: ProducerSettings[K, V])
                                                (implicit manager: ProducerManager[F]) {

  def simpleSink(topic: String): Sink[F, V] = { str =>
    useProducer(prod => {
      str.evalMap(v => {
        val record = new ProducerRecord[K, V](topic, v)
        prod.send(record)
      }).drain
    })
  }

  def pairSink(topic: String): Sink[F, (K, V)] = { str =>
    useProducer(prod => {
      str.evalMap(pair => {
        val (k, v) = pair
        val record = new ProducerRecord[K, V](topic, k, v)
        prod.send(record)
      }).drain
    })
  }

  def recordSink: Sink[F, ProducerRecord[K, V]] = { str =>
    useProducer(prod => {
      str
        .evalMap(rec => prod.send(rec))
        .drain
    })
  }

  private def useProducer[O](use: Producer[F, K, V] => Stream[F, Unit]) = {
    Stream.bracket(manager.createProducer(config))(use, _.close)
  }
}
