package gq.dsfn.kafkaesque.producer

import cats.effect.Async
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

trait Producer[F[_], K, V] {
  def send(record: ProducerRecord[K, V]): F[RecordMetadata]
  def close: F[Unit]
}

object Producer {
  def async[F[_], K, V](underlying: KafkaProducer[K, V])(implicit F: Async[F]): Producer[F, K, V] = {
    new Producer[F, K, V] {
      override def send(record: ProducerRecord[K, V]): F[RecordMetadata] = F.delay {
        underlying.send(record).get
      }

      override def close: F[Unit] = F.delay {
        underlying.close()
      }
    }
  }
}
