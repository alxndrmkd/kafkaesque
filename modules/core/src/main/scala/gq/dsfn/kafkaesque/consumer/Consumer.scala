package gq.dsfn.kafkaesque.consumer

import cats.effect.Async
import gq.dsfn.kafkaesque.settings.Subscription
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

trait Consumer[F[_], K, V] {
  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]]
  def commit: F[Unit]
  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]
  def subscribe(subscription: Subscription): F[Unit]
  def close: F[Unit]
}

object Consumer {
  def async[F[_], K, V](underlying: KafkaConsumer[K, V])(implicit F: Async[F]) = new Consumer[F, K, V] {
    override def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] = F.delay {
      underlying.poll(timeout.toMillis)
    }

    override def commit: F[Unit] = F.delay {
      underlying.commitSync()
    }

    override def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] = F.delay {
      underlying.commitSync(offsets.asJava)
    }

    override def subscribe(subscription: Subscription): F[Unit] = F.delay {
      underlying.subscribe(subscription.topics.asJava)
    }

    override def close: F[Unit] = F.delay {
      underlying.close()
    }
  }
}
