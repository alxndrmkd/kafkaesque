package gq.dsfn.kafkaesque.consumer

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

sealed trait Committable[F[_], R] {
  def record: R
  def commit: F[Unit]
}

object Committable {
  def single[F[_], K, V](record: ConsumerRecord[K, V], cons: Consumer[F, K, V]): Committable[F, ConsumerRecord[K, V]] = {
    new Committable[F, ConsumerRecord[K, V]] {
      override val record: ConsumerRecord[K, V] = record

      override def commit: F[Unit] = {
        val topicPartition = new TopicPartition(record.topic(), record.partition())
        cons.commit(
          Map(topicPartition -> new OffsetAndMetadata(record.offset()))
        )
      }
    }
  }

  def batch[F[_], K, V](records: ConsumerRecords[K, V], consumer: Consumer[F, K, V]): Committable[F, ConsumerRecords[K, V]] = {
    new Committable[F, ConsumerRecords[K, V]] {
      override def record: ConsumerRecords[K, V] = records

      override def commit: F[Unit] = {
        val topicPartitions = records
          .partitions()
          .asScala
          .toList

        val offsets = topicPartitions.flatMap(tp => {
          records.records(tp.topic())
            .asScala
            .lastOption
            .map(rec => tp -> new OffsetAndMetadata(rec.offset()))
        }).toMap

        consumer.commit(offsets)
      }
    }
  }
}
