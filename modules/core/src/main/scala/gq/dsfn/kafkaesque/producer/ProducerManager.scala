package gq.dsfn.kafkaesque.producer

import gq.dsfn.kafkaesque.settings.ProducerSettings

trait ProducerManager[F[_]] {
  def createProducer[K, V](config: ProducerSettings[K, V]): F[Producer[F, K, V]]
}
