package gq.dsfn.kafkaesque.settings

import org.apache.kafka.common.serialization.Serializer

final case class ProducerSettings[K, V](keySerializer: Serializer[K],
                                        valueSerializer: Serializer[V],
                                        properties: Map[String, String])
