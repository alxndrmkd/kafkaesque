package gq.dsfn

import java.util.Properties

import cats.effect.Async
import gq.dsfn.kafkaesque.consumer.{Consumer, ConsumerManager}
import gq.dsfn.kafkaesque.producer.{Producer, ProducerManager}
import gq.dsfn.kafkaesque.settings.{ConsumerSettings, ProducerSettings, Subscription}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import cats.implicits._
import scala.collection.JavaConverters._

package object kafkaesque {
  implicit def asyncConsumerManager[F[_]](implicit F: Async[F]) =
    new ConsumerManager[F] {
      override def createConsumer[K, V](config: ConsumerSettings[K, V],
                                        subscription: Subscription): F[Consumer[F, K, V]] = F.delay {
        val props = new Properties(){
          putAll(config.properties.asJava)
        }

        val underlying = new KafkaConsumer[K, V](props, config.keyDeserializer, config.valueDeserializer)

        underlying.subscribe(subscription.topics.asJava)

        Consumer.async(underlying)
      }
    }

  implicit def asyncProducerManager[F[_]](implicit F: Async[F]) =
    new ProducerManager[F] {
      override def createProducer[K, V](config: ProducerSettings[K, V]): F[Producer[F, K, V]] = F.delay {
        val props = new Properties(){
          putAll(config.properties.asJava)
        }

        val underlying = new KafkaProducer[K, V](props, config.keySerializer, config.valueSerializer)

        Producer.async(underlying)
      }
    }

}
