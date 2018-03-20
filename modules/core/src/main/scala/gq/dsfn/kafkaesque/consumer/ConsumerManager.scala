package gq.dsfn.kafkaesque.consumer

import gq.dsfn.kafkaesque.settings.{ConsumerSettings, Subscription}

trait ConsumerManager[F[_]] {
  def createConsumer[K, V](config: ConsumerSettings[K, V], subscription: Subscription): F[Consumer[F, K, V]]
}
