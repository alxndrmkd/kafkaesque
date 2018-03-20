package gq.dsfn.kafkaesque.settings

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer

final case class ConsumerSettings[K, V](keyDeserializer: Deserializer[K],
                                        valueDeserializer: Deserializer[V],
                                        properties: Map[String, String] = Map.empty) {
  def setBootstrapServers(bootstrapServers: String) = setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

  def setAutoCommit(autoCommit: Boolean) = setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit.toString)

  def setGroupId(groupId: String) = setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)

  def setMaxPollRecords(maxPollRecords: Long) = setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)

  def setAutoOffsetReset(reset: String) = setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, reset)

  def setClientId(clientId: String) = setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)

  private def setProperty(key: String, value: String) = {
    copy(properties = properties + (key -> value))
  }
}
