package gq.dsfn.kafkaesque.consumer

import cats.effect.Async
import fs2._
import gq.dsfn.kafkaesque.settings.{ConsumerSettings, Offset, Subscription}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

final case class ConsumerSource[F[_]: Async, K, V](config: ConsumerSettings[K, V],
                                                   subscriptions: Subscription = Subscription())
                                                  (implicit manager: ConsumerManager[F]) {

  def subscribe(topic: String) =
    copy(subscriptions = subscriptions.put(topic))

  def subscribe(assignment: TopicPartition) =
    copy(subscriptions = subscriptions.put(assignment))

  def subscribe(assignmentWithOffset: (TopicPartition, Offset)) = {
    copy(subscriptions = subscriptions.put(assignmentWithOffset))
  }

  def plainStream(pollInterval: FiniteDuration): Stream[F, ConsumerRecord[K, V]] = {
    useConsumer(cons => {
      Stream
        .repeatEval(cons.poll(pollInterval))
        .flatMap(rs => Stream.fromIterator(rs.iterator().asScala))
    })
  }

  def plainBatchStream(pollInterval: FiniteDuration): Stream[F, ConsumerRecords[K, V]] = {
    useConsumer(cons => {
      Stream
        .repeatEval(cons.poll(pollInterval))
    })
  }

  def committableStream(pollInterval: FiniteDuration): Stream[F, Committable[F, ConsumerRecord[K, V]]] = {
    useConsumer(cons => {
      Stream
        .repeatEval(cons.poll(pollInterval))
        .flatMap(records => Stream.fromIterator(records.iterator().asScala))
        .map(record => Committable.single(record, cons))
    })
  }

  def committableBatchStream(pollInterval: FiniteDuration): Stream[F, Committable[F, ConsumerRecords[K, V]]] = {
    useConsumer(cons => {
      Stream
        .repeatEval(cons.poll(pollInterval))
        .map(records => Committable.batch(records, cons))
    })
  }
  

  private def useConsumer[O](use: Consumer[F, K, V] => Stream[F, O]): Stream[F, O] = {
    Stream.bracket(manager.createConsumer(config, subscriptions))(use, _.close)
  }
}
